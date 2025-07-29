#include "EventLoop.h"
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>
#include "base/LogStream.h"
#include "base/Time.h"

using namespace nest::net;
using namespace nest::base;

static thread_local EventLoop *t_local_eventloop = nullptr;

EventLoop::EventLoop()
: ep_(new Epoller)
{
    if (t_local_eventloop) {
        LOG_ERROR << "Alread had a eventloop.";
        exit(-1);
    }
    t_local_eventloop = this;
}

EventLoop::~EventLoop()
{
    Quit();
}

void EventLoop::Loop()
{
    isLooping_ = true;
    int64_t timeout = 1000;
    while (isLooping_) {
        std::vector<ChannelPtr> channels = ep_->Poll(timeout);
        for (auto &ch : channels) {
            ch->HandleEvent();
        }
        RunFuctions();
        int64_t now = nest::base::Time::NowMS();
        wheel_.OnTimer(now);
    }
}

void EventLoop::Quit()
{
    isLooping_ = false;
}

void EventLoop::AddChannel(const ChannelPtr &channel)
{
    channel->event_ |= kEventRead;
    ep_->AddChannel(channel);
}

void EventLoop::DelChannel(const ChannelPtr &channel)
{
    ep_->RemoveChannel(channel);
}

bool EventLoop::EnableChannelWriting(const ChannelPtr &channel, bool enable)
{
    if (enable) {
        channel->event_ |= kEventWrite;
    } else {
        channel->event_ &= ~kEventWrite;
    }
    ep_->UpdateChannel(channel);
    return true;
}

bool EventLoop::EnableChannelReading(const ChannelPtr &channel, bool enable)
{
    if (enable) {
        channel->event_ |= kEventRead;
    } else {
        channel->event_ &= ~kEventRead;
    }
    ep_->UpdateChannel(channel);
    return true;
}

void EventLoop::AssertInLoopThread()
{
    if(!IsInLoopThread()) {
        LOG_ERROR << "It is forbidden to run loop on other thread!";
        exit(-1);
    }
}

bool EventLoop::IsInLoopThread() const
{
    return this == t_local_eventloop;
}

void EventLoop::RunInLoop(const std::function<void()> &fn)
{
    if (IsInLoopThread()) {
        fn();
    } else {
        std::lock_guard<std::mutex> lk(mtx_);
        fns_.push(fn);

        WakeUp();
    }
}

void EventLoop::RunInLoop(std::function<void()> &&fn)
{
    if (IsInLoopThread()) {
        fn();
    } else {
        std::lock_guard<std::mutex> lk(mtx_);
        fns_.push(std::move(fn));

        WakeUp();
    }
}

void EventLoop::RunFuctions()
{
    std::lock_guard<std::mutex> lk(mtx_);
    while (!fns_.empty()) {
        const std::function<void()> &f = fns_.front();
        f();
        fns_.pop();
    }
}

void EventLoop::WakeUp()
{
    if(!pipeEvent_) {
        pipeEvent_ = std::make_shared<PipeEvent>(this);
        AddChannel(pipeEvent_);
    }
    int64_t tmp = 1;
    pipeEvent_->Write((const char*)&tmp, sizeof(tmp));
}

void EventLoop::InsertEntry(uint32_t delay, EntryPtr entryPtr)
{
    if(IsInLoopThread()) {
        wheel_.InsertEntry(delay, entryPtr);
    } else {
        RunInLoop([this, delay, entryPtr]{
            wheel_.InsertEntry(delay, entryPtr);
        });
    }
}

void EventLoop::RunAfter(double delay, const std::function<void()> &cb)
{
    if(IsInLoopThread()) {
        wheel_.RunAfter(delay, cb);
    } else {
        RunInLoop([this,delay,cb]{
            wheel_.RunAfter(delay, cb);
        });
    }
}

void EventLoop::RunAfter(double delay, std::function<void()> &&cb)
{
    if(IsInLoopThread()) {
        wheel_.RunAfter(delay, cb);
    } else {
        RunInLoop([this,delay,cb]{
            wheel_.RunAfter(delay, cb);
        });
    }
}

void EventLoop::RunEvery(double interval, const std::function<void()> &cb)
{
    if(IsInLoopThread()) {
        wheel_.RunEvery(interval, cb);
    } else {
        RunInLoop([this,interval,cb]{
            wheel_.RunEvery(interval, cb);
        });
    }
}

void EventLoop::RunEvery(double interval, std::function<void()> &&cb)
{
    if(IsInLoopThread()) {
        wheel_.RunEvery(interval, cb);
    } else {
        RunInLoop([this,interval,cb]{
            wheel_.RunEvery(interval, cb);
        });
    }
}