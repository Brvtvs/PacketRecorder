package io.brutus.packetrecorder.util;

/**
 * Debug message management.
 */
public class Debug
{

    private static boolean enabled = false;

    /**
     * Gets whether debug messaging is currently enabled.
     *
     * @return <code>true</code> if debug messaging is enabled, else <code>false</code>.
     */
    public static boolean isEnabled()
    {
        return enabled;
    }

    /**
     * Prints a debug message if debug messages are enabled.
     *
     * @param loggingObject The class logging the message.
     * @param message The debug message to print.
     */
    public static void out(Object loggingObject, String message)
    {
        if (enabled && message != null && !message.isEmpty())
        {
            String prefix = loggingObject != null ? "[" + loggingObject.getClass().getSimpleName() + "] " : "";
            System.out.println(prefix + message);
        }
    }

}
