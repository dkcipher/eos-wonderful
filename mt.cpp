
#include <algorithm>
#include <iostream>
#include <map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/common/filesystem.h"
#include "utils/common/functions.h"
#include "utils/common/exception.h"

#include "app/data_sourcer/bridge_sender_impl.h"
#include "app/data_sourcer/flush_status_impl.h"
#include "app/data_sourcer/balancer.h"

#include "test/app/bridge/mock_client.h"
#include "mock_queue.h"
#include "mock_data_reader.h"
#include "mock_mapper.h"
#include "unittest_utils.h"

using namespace ::breeze;
using namespace ::breeze::app;
using namespace ::breeze::app::data_sourcer;

using ::testing::_;
using ::testing::An;
using ::testing::Return;
using ::testing::Throw;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::SetArgReferee;
using ::testing::SetArgPointee;
using ::testing::SaveArgPointee;

class DataSourcerBridgeSenderTest : public ::testing::Test
{
protected:
    typedef std::pair<uint64_t, uint64_t> SNPair;
    typedef std::pair<uint32_t, std::string> MockData;
    typedef std::map<SNPair, MockData> MockDataMap;

    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }

    // make dummy queue items.
    void make_dummy_item(QueueItem& item, QueueStoreType type, uint32_t id, uint64_t min, uint64_t max)
    {

        item.min_store_sn = min;
        item.max_store_sn = max;
        item.queue_type = type;
        item.id = id;

        if (type == UnicastItem or type == BroadcastItem)
        {
            item.data_type = (type == UnicastItem)?
                IndexingCommand::UPDATE : IndexingCommand::CONDITIONAL_UPDATE;
            std::string type_name = get_idxcmd_name(item.data_type, "unknown");
            std::ostringstream data;
            data << "[";
            for (uint64_t sn = min; sn <= max; ++sn)
            {
                if (sn > min)
                    data << ",";
                data << "{\"sn\":" << sn <<
                    ",\"type\":\"" << type_name << "\"}";
            }
            data << "]";

            SNPair pair(min, max);
            MockData& mock_data = m_mock_data[pair];
            // id == fetch_id
            mock_data.first = id;
            mock_data.second = data.str();

            item.data_size = mock_data.second.size();
            item.document_count = max - min + 1;
        }
    }

    // make SN list.
    void make_sn_list(std::vector<uint64_t>& sn_list, uint64_t offset, uint64_t count)
    {
        uint64_t end = offset + count;
        for (; offset < end; ++offset)
        {
            sn_list.push_back(offset);
        }
        std::random_shuffle(sn_list.begin(), sn_list.end());
    }

    // bridge client.
    bool mock_client_post_indexing_command(const bridge::Cluster &cluster, const bridge::ICU &icu, int64_t &sn)
    {
        sn = m_sn;
        ++m_sn;
        return true;
    }

    // data reader.
    void mock_data_reader_read(uint32_t cid, QueueItem& item, FlushedInfo& finfo, std::string& data)
    {
        if (item.queue_type == EmptyItem or item.queue_type == PivotItem)
        {
            FlushedInfo::Range range(item.min_store_sn, item.max_store_sn);
            finfo.set(item.id, range);
            if (item.queue_type == PivotItem)
            {
                finfo.set_pivot(true);
            }
        }
        else
        {
            // set data.
            SNPair pair(item.min_store_sn, item.max_store_sn);
            MockData& mock_data = m_mock_data[pair];
            data = mock_data.second;
            // update flushed info.
            // store_sn == fetched_sn
            for (uint64_t sn = pair.first; sn <= pair.second; ++sn)
            {
                finfo.put(mock_data.first, sn);
            }
        }
    }

    void init_mock_objects()
    {
        // cluster ids.
        m_cluster_ids.clear();
        for (unsigned i = 0; i < CLUSTER_COUNT; ++i)
            m_cluster_ids.insert(i);

        // bridge client.
        EXPECT_CALL(m_bridge_client, post_indexing_command(_, _, _))
            .WillRepeatedly(Invoke(this, &DataSourcerBridgeSenderTest::mock_client_post_indexing_command));
        EXPECT_CALL(m_bridge_client, full(_, _))
            .WillRepeatedly(Return(false));

        // data reader.
        MockQueueDataReader* data_reader = new MockQueueDataReader();
        EXPECT_CALL(*data_reader, read(_, _, _, _))
            .WillRepeatedly(Invoke(this, &DataSourcerBridgeSenderTest::mock_data_reader_read));
        EXPECT_CALL(*data_reader, remove(_, _))
            .WillRepeatedly(Return());
        m_data_reader.reset(data_reader);
        m_mock_data.clear();

        // flush status.
        m_flush_status.reset(new FlushStatusImpl());
    }

protected:
    bridge::MockClient m_bridge_client;
    QueueDataReaderPtr m_data_reader;
    NullBalancer       m_balancer;
    FlushStatusPtr     m_flush_status;
    std::set<int>      m_cluster_ids;
    int64_t            m_sn;
    MockDataMap        m_mock_data;

    static const unsigned FETCHER_COUNT;
    static const unsigned GROUP_COUNT;
    static const unsigned CLUSTER_COUNT;
    static const uint64_t SN_OFFSET;
};

const unsigned DataSourcerBridgeSenderTest::FETCHER_COUNT = 10;
const unsigned DataSourcerBridgeSenderTest::GROUP_COUNT = 15;
const unsigned DataSourcerBridgeSenderTest::CLUSTER_COUNT = 24;
const uint64_t DataSourcerBridgeSenderTest::SN_OFFSET = 200;

TEST_F(DataSourcerBridgeSenderTest, empty_item)
{
    try
    {
        init_mock_objects();
        BridgeSenderImpl sender(10, &m_bridge_client, m_data_reader, &m_balancer, m_flush_status, m_cluster_ids);

        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            EXPECT_EQ(0u, sender.get_sent_sn(fetcher));
            EXPECT_EQ(0u, sender.get_pivot_sn(fetcher));
        }

        // insert even items.
        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            std::vector<uint64_t> sn_list;
            make_sn_list(sn_list, (fetcher + 1) * SN_OFFSET, CLUSTER_COUNT / 2);

            for (unsigned cluster = 0; cluster < CLUSTER_COUNT; cluster += 2)
            {
                QueueItem item;
                uint64_t sn = sn_list[cluster / 2];
                make_dummy_item(item, EmptyItem, fetcher, sn, sn);
                sender.send(cluster, item);
            }
        }

        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            EXPECT_EQ(0u, sender.get_sent_sn(fetcher));
            EXPECT_EQ(0u, sender.get_pivot_sn(fetcher));
        }

        // insert odd items.
        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            std::vector<uint64_t> sn_list;
            make_sn_list(sn_list, (fetcher + 1) * SN_OFFSET + CLUSTER_COUNT / 2, CLUSTER_COUNT / 2);

            for (unsigned cluster = 1; cluster < CLUSTER_COUNT; cluster += 2)
            {
                QueueItem item;
                uint64_t sn = sn_list[cluster / 2];
                make_dummy_item(item, EmptyItem, fetcher, sn, sn);
                sender.send(cluster, item);
            }
        }

        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            EXPECT_EQ((fetcher + 1) * SN_OFFSET, sender.get_sent_sn(fetcher));
            EXPECT_EQ(0u, sender.get_pivot_sn(fetcher));
        }
    }
    catch (const utils::CException& ex)
    {
        EXPECT_TRUE(false) << "empty_item failed: " << ex.what();
    }
}

TEST_F(DataSourcerBridgeSenderTest, pivot_item)
{
    try
    {
        init_mock_objects();
        BridgeSenderImpl sender(10, &m_bridge_client, m_data_reader, &m_balancer, m_flush_status, m_cluster_ids);
        // insert even items.
        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            std::vector<uint64_t> sn_list;
            make_sn_list(sn_list, (fetcher + 1) * SN_OFFSET, CLUSTER_COUNT / 2);

            for (unsigned cluster = 0; cluster < CLUSTER_COUNT; cluster += 2)
            {
                QueueItem item;
                uint64_t sn = sn_list[cluster / 2];
                make_dummy_item(item, PivotItem, fetcher, sn, sn);
                sender.send(cluster, item);
            }
        }

        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            EXPECT_EQ(0u, sender.get_sent_sn(fetcher));
            EXPECT_EQ(0u, sender.get_pivot_sn(fetcher));
        }

        // insert odd items.
        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            std::vector<uint64_t> sn_list;
            make_sn_list(sn_list, (fetcher + 1) * SN_OFFSET + CLUSTER_COUNT / 2, CLUSTER_COUNT / 2);
            for (unsigned cluster = 1; cluster < CLUSTER_COUNT; cluster += 2)
            {
                QueueItem item;
                uint64_t sn = sn_list[cluster / 2];
                make_dummy_item(item, PivotItem, fetcher, sn, sn);
                sender.send(cluster, item);
            }
        }

        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            EXPECT_EQ((fetcher + 1) * SN_OFFSET, sender.get_sent_sn(fetcher));
            EXPECT_EQ((fetcher + 1) * SN_OFFSET, sender.get_pivot_sn(fetcher));
        }
    }
    catch (const utils::CException& ex)
    {
        EXPECT_TRUE(false) << "pivot_item failed: " << ex.what();
    }
}

TEST_F(DataSourcerBridgeSenderTest, normal_item)
{
    try
    {
        init_mock_objects();
        BridgeSenderImpl sender(10, &m_bridge_client, m_data_reader, &m_balancer, m_flush_status, m_cluster_ids);
        // insert even items.
        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            std::vector<uint64_t> sn_list;
            make_sn_list(sn_list, (fetcher + 1) * SN_OFFSET, CLUSTER_COUNT / 2);

            for (unsigned cluster = 0; cluster < CLUSTER_COUNT; cluster += 2)
            {
                QueueItem item;
                uint64_t sn = sn_list[cluster / 2];
                make_dummy_item(item, BroadcastItem, fetcher, sn, sn);
                sender.send(cluster, item);
            }
        }

        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            EXPECT_EQ(0u, sender.get_sent_sn(fetcher));
            EXPECT_EQ(0u, sender.get_pivot_sn(fetcher));
        }

        // insert odd items.
        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            std::vector<uint64_t> sn_list;
            make_sn_list(sn_list, (fetcher + 1) * SN_OFFSET + CLUSTER_COUNT / 2, CLUSTER_COUNT / 2);
            for (unsigned cluster = 1; cluster < CLUSTER_COUNT; cluster += 2)
            {
                QueueItem item;
                uint64_t sn = sn_list[cluster / 2];
                make_dummy_item(item, UnicastItem, fetcher, sn, sn);
                sender.send(cluster, item);
            }
        }

        for (unsigned fetcher = 0; fetcher < FETCHER_COUNT; ++fetcher)
        {
            EXPECT_EQ((fetcher + 1) * SN_OFFSET, sender.get_sent_sn(fetcher));
            EXPECT_EQ(0u, sender.get_pivot_sn(fetcher));
        }
    }
    catch (const utils::CException& ex)
    {
        EXPECT_TRUE(false) << "normal_item failed: " << ex.what();
    }
}

TEST_F(DataSourcerBridgeSenderTest, invalid_cluster)
{
    init_mock_objects();
    BridgeSenderImpl sender(10, &m_bridge_client, m_data_reader, &m_balancer, m_flush_status, m_cluster_ids);
    // send to invalid cluster.
    QueueItem item;
    uint32_t fetcher = 1, cluster_id = CLUSTER_COUNT + 1;
    uint64_t sn = 5;
    make_dummy_item(item, BroadcastItem, fetcher, sn, sn);
    EXPECT_THROW(sender.send(cluster_id, item), utils::CException);
}

TEST_F(DataSourcerBridgeSenderTest, exception)
{
    init_mock_objects();
    BridgeSenderImpl sender(10, &m_bridge_client, m_data_reader, &m_balancer, m_flush_status, m_cluster_ids);
    // send to invalid cluster.
    QueueItem item;
    uint32_t fetcher = 1, cluster_id = 0;
    uint64_t sn = 5;
    std::string err_str = "test exception";
    make_dummy_item(item, BroadcastItem, fetcher, sn, sn);

    // throw proxy exception with code.
    EXPECT_CALL(m_bridge_client, post_indexing_command(_, _, _))
        .WillRepeatedly(Throw(bridge::ProxyException(bridge::ProxyException::TemporaryUnavailable, err_str)));
    ASSERT_NO_THROW(sender.send(cluster_id, item));
    EXPECT_TRUE(sender.get_last_error(cluster_id).find(err_str) != std::string::npos);

    // throw proxy exception with code (unknown).
    EXPECT_CALL(m_bridge_client, post_indexing_command(_, _, _))
        .WillRepeatedly(Throw(bridge::ProxyException(bridge::ProxyException::NotSupported, err_str)));
    ASSERT_NO_THROW(sender.send(cluster_id, item));
    EXPECT_TRUE(sender.get_last_error(cluster_id).find(err_str) != std::string::npos);

    // throw CException.
    EXPECT_CALL(m_bridge_client, post_indexing_command(_, _, _))
        .WillRepeatedly(Throw(utils::CException(err_str)));
    ASSERT_NO_THROW(sender.send(cluster_id, item));
    EXPECT_TRUE(sender.get_last_error(cluster_id).find(err_str) != std::string::npos);

    // throw runtime exception.
    EXPECT_CALL(m_bridge_client, post_indexing_command(_, _, _))
        .WillRepeatedly(Throw(std::runtime_error(err_str)));
    ASSERT_NO_THROW(sender.send(cluster_id, item));
    EXPECT_TRUE(not sender.get_last_error(cluster_id).empty());
}

TEST_F(DataSourcerBridgeSenderTest, is_full)
{
    try
    {
        init_mock_objects();
        EXPECT_CALL(m_bridge_client, full(_, _))
            .WillOnce(Return(false))
            .WillOnce(Return(false))
            .WillOnce(Return(true))
            .WillOnce(Return(false))
            .WillOnce(Return(true))
            .WillOnce(Return(true))
            .WillOnce(Return(false))
            .WillOnce(Return(true))
            .WillOnce(Return(false))
            .WillOnce(Return(true));

        BridgeSenderImpl sender(10, &m_bridge_client, m_data_reader, &m_balancer, m_flush_status, m_cluster_ids);
        unsigned true_count = 0, false_count = 0;
        for (unsigned trial = 0; trial < 10; ++trial)
        {
            if (sender.is_full(10))
                ++true_count;
            else
                ++false_count;
        }
        EXPECT_EQ(5u, true_count);
        EXPECT_EQ(5u, false_count);
    }
    catch (const utils::CException& ex)
    {
        EXPECT_TRUE(false) << "is_full failed: " << ex.what();
    }
}

