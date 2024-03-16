package com.integration.em.utils;

import java.util.Calendar;

public class CalendarUtil {

    private CalendarUtil() {
    }

    public static boolean isValidDate(int year, int month, int day) {
        return isValidDate(year, month, day, 0, 0, 0);
    }

    public static boolean isValidTime(int hour, int minute, int second) {
        return isValidDate(1, 1, 1, hour, minute, second);
    }

    public static boolean isValidDate(
            int year, int month, int day, int hour, int minute, int second)
    {
        try {
            getValidCalendar(year, month, day, hour, minute, second);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public static Calendar getValidCalendar(int year, int month, int day) {
        return getValidCalendar(year, month, day, 0, 0, 0);
    }

    public static Calendar getValidCalendar(
            int year, int month, int day, int hour, int minute, int second)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setLenient(false); // Don't automatically convert invalid date.
        calendar.set(year, month - 1, day, hour, minute, second);
        calendar.getTimeInMillis(); // Lazy update, throws IllegalArgumentException if invalid date.
        return calendar;
    }

    public static void addYears(Calendar calendar, int years) {
        calendar.add(Calendar.YEAR, years);
    }

    public static void addMonths(Calendar calendar, int months) {
        calendar.add(Calendar.MONTH, months);
    }

    public static void addDays(Calendar calendar, int days) {
        calendar.add(Calendar.DATE, days);
    }


    public static void addHours(Calendar calendar, int hours) {
        calendar.add(Calendar.HOUR, hours);
    }


    public static void addMinutes(Calendar calendar, int minutes) {
        calendar.add(Calendar.MINUTE, minutes);
    }


    public static void addSeconds(Calendar calendar, int seconds) {
        calendar.add(Calendar.SECOND, seconds);
    }


    public static void addMillis(Calendar calendar, int millis) {
        calendar.add(Calendar.MILLISECOND, millis);
    }


    public static boolean sameYear(Calendar one, Calendar two) {
        return one.get(Calendar.YEAR) == two.get(Calendar.YEAR);
    }


    public static boolean sameMonth(Calendar one, Calendar two) {
        return one.get(Calendar.MONTH) == two.get(Calendar.MONTH) && sameYear(one, two);
    }


    public static boolean sameDay(Calendar one, Calendar two) {
        return one.get(Calendar.DATE) == two.get(Calendar.DATE) && sameMonth(one, two);
    }


    public static boolean sameHour(Calendar one, Calendar two) {
        return one.get(Calendar.HOUR_OF_DAY) == two.get(Calendar.HOUR_OF_DAY) && sameDay(one, two);
    }


    public static boolean sameMinute(Calendar one, Calendar two) {
        return one.get(Calendar.MINUTE) == two.get(Calendar.MINUTE) && sameHour(one, two);
    }


    public static boolean sameSecond(Calendar one, Calendar two) {
        return one.get(Calendar.SECOND) == two.get(Calendar.SECOND) && sameMinute(one, two);
    }


    public static boolean sameTime(Calendar one, Calendar two) {
        return one.getTimeInMillis() == two.getTimeInMillis();
    }


    public static int elapsedYears(Calendar before, Calendar after) {
        return elapsed(before, after, Calendar.YEAR);
    }


    public static int elapsedMonths(Calendar before, Calendar after) {
        return elapsed(before, after, Calendar.MONTH);
    }


    public static int elapsedDays(Calendar before, Calendar after) {
        return elapsed(before, after, Calendar.DATE);
    }


    public static int elapsedHours(Calendar before, Calendar after) {
        return (int) elapsedMillis(before, after, 3600000); // 1h = 60m = 3600s = 3600000ms
    }


    public static int elapsedMinutes(Calendar before, Calendar after) {
        return (int) elapsedMillis(before, after, 60000); // 1m = 60s = 60000ms
    }


    public static int elapsedSeconds(Calendar before, Calendar after) {
        return (int) elapsedMillis(before, after, 1000); // 1sec = 1000ms.
    }


    public static long elapsedMillis(Calendar before, Calendar after) {
        return elapsedMillis(before, after, 1); // 1ms is apparently 1ms.
    }


    public static int[] elapsedTime(Calendar before, Calendar after) {
        int[] elapsedTime = new int[6];
        Calendar clone = (Calendar) before.clone(); // Otherwise changes are been reflected.

        elapsedTime[0] = elapsedYears(clone, after);
        addYears(clone, elapsedTime[0]);

        elapsedTime[1] = elapsedMonths(clone, after);
        addMonths(clone, elapsedTime[1]);

        elapsedTime[2] = elapsedDays(clone, after);
        addDays(clone, elapsedTime[2]);

        elapsedTime[3] = elapsedHours(clone, after);
        addHours(clone, elapsedTime[3]);

        elapsedTime[4] = elapsedMinutes(clone, after);
        addMinutes(clone, elapsedTime[4]);

        elapsedTime[5] = elapsedSeconds(clone, after);

        return elapsedTime;
    }


    private static int elapsed(Calendar before, Calendar after, int field) {
        checkBeforeAfter(before, after);
        Calendar clone = (Calendar) before.clone(); // Otherwise changes are been reflected.
        int elapsed = -1;
        while (!clone.after(after)) {
            clone.add(field, 1);
            elapsed++;
        }
        return elapsed;
    }


    private static long elapsedMillis(Calendar before, Calendar after, int factor) {
        checkBeforeAfter(before, after);
        if (factor < 1) {
            throw new IllegalArgumentException(
                    "Division factor '" + factor + "' should not be less than 1.");
        }
        return (after.getTimeInMillis() - before.getTimeInMillis()) / factor;
    }


    private static void checkBeforeAfter(Calendar before, Calendar after) {
        if (before.after(after)) {
            throw new IllegalArgumentException(
                    "The first calendar should be dated before the second calendar.");
        }
    }

}
