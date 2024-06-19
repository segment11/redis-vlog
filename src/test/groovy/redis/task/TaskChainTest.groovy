package redis.task

import spock.lang.Specification

class TaskChainTest extends Specification {
    static class Task1 implements ITask {
        @Override
        String name() {
            'task1'
        }

        @Override
        void run() {
            println 'task1, loop count: ' + loopCount
        }

        int loopCount

        @Override
        void setLoopCount(int loopCount) {
            this.loopCount = loopCount
        }
    }

    def 'test all'() {
        given:
        def taskChain = new TaskChain()
        def task1 = new Task1()

        when:
        taskChain.add(task1)

        then:
        taskChain.list.size() == 1

        when:
        10.times {
            taskChain.doTask(it)
        }

        then:
        task1.loopCount == 9

        when:
        taskChain.remove('task1')

        then:
        taskChain.list.size() == 0
    }
}
