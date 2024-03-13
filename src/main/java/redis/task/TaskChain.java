package redis.task;

import java.util.ArrayList;

public class TaskChain {
    // need not thread safe
    public final ArrayList<ITask> list = new ArrayList<>();

    public ArrayList<ITask> list() {
        return list;
    }

    public void add(ITask task) {
        for (ITask t : list) {
            if (t.name().equals(task.name())) {
                return;
            }
        }

        list.add(task);
    }

    public ITask remove(String name) {
        var it = list.iterator();
        while (it.hasNext()) {
            var t = it.next();
            if (t.name().equals(name)) {
                it.remove();
                return t;
            }
        }
        return null;
    }
}
