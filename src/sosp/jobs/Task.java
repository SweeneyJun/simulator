package sosp.jobs;

public class Task {
    // predefined constants
    public Job _job = null;
    public double computationDelay = 0; // s

    // runtime constants (modified by only once)
    public int host = -1;
    public double startTime = -1;
}
