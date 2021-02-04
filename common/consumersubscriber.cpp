#include "consumersubscriber.h"
#include "redisreply.h"

using namespace swss;
using namespace std;


 ConsumerSubscriber::ConsumerSubscriber(DBConnector* db, const std::string &tableName, int pri)
        : TableBase(tableName, SonicDBConfig::getSeparator(db)),
          TableName_KeyValueOpQueues(tableName),
          RedisSelect(pri)
{
    psubscribe(db, getChannelNamePattern());

    RedisPipeline pipe(db, 1);
    RedisCommand keys_cmd;
    keys_cmd.format("KEYS %s", getKeyValueOpQueuePattern().c_str());
    RedisReply r = pipe.push(keys_cmd, REDIS_REPLY_ARRAY);
    redisReply *reply = r.getContext();

    SWSS_LOG_ERROR("LOG_MARKER elements in reply %s %zu", keys_cmd.c_str(), reply->elements);

    for (unsigned int i = 0; i < reply->elements; i++)
    {
        string key = reply->element[i]->str;
        SWSS_LOG_ERROR("LOG_MARKER Put into queue %s", key.c_str());
        m_init_buffer.push_back(parseKeyValueOpQueuePattern(key));
    }
}

bool ConsumerSubscriber::initializedWithData()
{
    return !m_init_buffer.empty();
}

uint64_t ConsumerSubscriber::readData()
{
    redisReply *reply = nullptr;

    /* Read data from redis. This call is non blocking. This method
     * is called from Select framework when data is available in socket.
     * NOTE: All data should be stored in event buffer. It won't be possible to
     * read them second time. */
    if (redisGetReply(m_subscribe->getContext(), reinterpret_cast<void**>(&reply)) != REDIS_OK)
    {
        throw std::runtime_error("Unable to read redis reply");
    }

    m_buffer.emplace_back(make_shared<RedisReply>(reply));

    /* Try to read data from redis cacher.
     * If data exists put it to event buffer.
     * NOTE: Keyspace event is not persistent and it won't
     * be possible to read it second time. If it is not stared in
     * the buffer it will be lost. */

    reply = nullptr;
    int status;
    do
    {
        status = redisGetReplyFromReader(m_subscribe->getContext(), reinterpret_cast<void**>(&reply));
        if(reply != nullptr && status == REDIS_OK)
        {
            m_buffer.emplace_back(make_shared<RedisReply>(reply));
        }
    }
    while(reply != nullptr && status == REDIS_OK);

    if (status != REDIS_OK)
    {
        throw std::runtime_error("Unable to read redis reply");
    }
    return 0;
}

bool ConsumerSubscriber::hasData()
{
    return (m_init_buffer.size() > 0) || (m_buffer.size() > 0);
}

bool ConsumerSubscriber::hasCachedData()
{
    return m_init_buffer.size() + m_buffer.size() > 1;
}

shared_ptr<RedisReply> ConsumerSubscriber::popEventBuffer()
{
    if (m_buffer.empty())
    {
        return NULL;
    }

    auto reply = m_buffer.front();
    m_buffer.pop_front();

    return reply;
}

bool ConsumerSubscriber::getReadyConsumerName(std::string& name)
{
    if (!m_init_buffer.empty())
    {
        name = m_init_buffer.front();
        m_init_buffer.pop_front();
        return true;
    }

    auto event = popEventBuffer();
    /* if the Key-space notification is empty, try next one. */
    auto message = event->getReply<RedisMessage>();
    if (message.type.empty())
    {
        return false;
    }

    if (message.pattern != getChannelNamePattern())
    {
        SWSS_LOG_ERROR("invalid pattern %s returned for pmessage of %s", message.pattern.c_str(), getChannelNamePattern().c_str());
        return false;
    }

    name = parseNameFromChannel(message.channel);
    return true;
}
