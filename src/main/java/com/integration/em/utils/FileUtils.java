package com.integration.em.utils;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class FileUtils {

    public static long countLines(String path) {

        long lines=0;

        try {
            BufferedReader r = new BufferedReader(getReader(path));


            while(r.readLine()!=null)
            {
                lines++;
            }

            r.close();
        } catch (IOException e) {
            e.printStackTrace();
            lines=-1;
        }

        return lines;
    }

    public static Reader getReader(String path) throws FileNotFoundException, IOException {
        if (path.endsWith(".gz")) {
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(path));
            return new InputStreamReader(gzip, "UTF-8");
        } else {
            return new InputStreamReader(new FileInputStream(path), "UTF-8");
        }
    }


    public static List<File> listAllFiles(File root) {
        LinkedList<File> files = new LinkedList<>();

        if(root.isDirectory()) {
            LinkedList<File> toList = new LinkedList<>(Arrays.asList(root.listFiles()));

            while(!toList.isEmpty()) {
                File f = toList.poll();

                if(f.isDirectory()) {
                    toList.addAll(Arrays.asList(f.listFiles()));
                } else {
                    files.add(f);
                }
            }
        } else {
            files.add(root);
        }

        return files;
    }
}
