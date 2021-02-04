#pragma once

#include "redisselect.h"
#include "table.h"
#include "dbconnector.h"

namespace swss
{

// TODO: This class duplicates SubscriberStateTable a lot.
//       Need to create reusable base class or component.
class ConsumerSubscriber : public TableBase, public TableName_KeyValueOpQueues, public RedisSelect
{
public:
    ConsumerSubscriber(DBConnector* db, const std::string &tableName, int pri = 0);

    bool getReadyConsumerName(std::string&);

    uint64_t readData() override;
    bool hasData() override;
    bool hasCachedData() override;
    bool initializedWithData() override;

private:
    std::shared_ptr<RedisReply> popEventBuffer();

    std::deque<std::string> m_init_buffer;
    std::deque<std::shared_ptr<RedisReply>> m_buffer;
};

}