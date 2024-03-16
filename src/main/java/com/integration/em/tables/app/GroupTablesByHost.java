package com.integration.em.tables.app;

import com.beust.jcommander.Parameter;

import com.integration.em.tables.Tbl;
import com.integration.em.tables.UriParser;
import com.integration.em.utils.Executable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;

@Slf4j
public class GroupTablesByHost extends Executable {

	@Parameter(names = "-tables", required=true)
	private String input;
	
	@Parameter(names = "-result", required=true)
	private String output;
	
	@Parameter(names = "-copy")
	private boolean copy;

	public static void main(String[] args) throws Exception {
		
		GroupTablesByHost app = new GroupTablesByHost();
		
		if(app.parseCommandLine(GroupTablesByHost.class, args)) {
			
			app.run();
			
		}
		
	}
	
	public void run() throws Exception {
		
		File in = new File(input);
		File out = new File(output);
		
		if(!in.exists()) {
			log.error(String.format("%s does not exist!", in.getAbsolutePath()));
			return;
		}
		
		if(!out.exists()) {
			out.mkdirs();
		}
		
		log.info(in.getAbsolutePath());
		
		LinkedList<File> files = new LinkedList<>();


		Path path = in.toPath();
		Files.walkFileTree(path, new SimpleFileVisitor<Path>() {

			long last = System.currentTimeMillis();

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				files.add(file.toFile());
				if(System.currentTimeMillis()-last>10000) {
					log.info(String.format("Listing files, %d so far ...", files.size()));
					last = System.currentTimeMillis();
				}
				return FileVisitResult.CONTINUE;
			}
		});
		
		UriParser p = new UriParser();
		long start = System.currentTimeMillis();
		long lastTime = 0;
		int ttlFiles = files.size();
		int last = 0;
		
		while(!files.isEmpty()) {
			File f = files.poll();
			

				Tbl t = p.parseTbl(f);
				
				File newFile = new File(new File(out, getHostName(t)), t.getPath());
				newFile.getParentFile().mkdir();
				
				if(copy) {
					Files.copy(f.toPath(), newFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				} else {
					Files.createLink(newFile.toPath(), f.toPath());
				}
				
			// }
			

			if(System.currentTimeMillis()-lastTime>=10000) {
				
				int tasks = ttlFiles;
				int done = ttlFiles - files.size();
				
				long soFar = System.currentTimeMillis() - start;
				long pauseTime = System.currentTimeMillis() - lastTime;
				long left = (long) (((float) soFar / done) * (tasks - done));
				float itemsPerSecAvg = (float)done / (float)(soFar / 1000.0f);
				float itemsPerSecNow = (float)(done - last) / (pauseTime / 1000.0f);
				
				if((((float) soFar) / done)==Float.POSITIVE_INFINITY)
				{
					left = -1;
				}
				String ttl = DurationFormatUtils.formatDuration(soFar, "HH:mm:ss.S");
				String remaining = DurationFormatUtils.formatDuration(left, "HH:mm:ss.S");
				
				log.info(String.format("%,d of %,d tasks completed after %s. Avg: %.2f items/s, Current: %.2f items/s, %s left.", done, tasks, ttl, itemsPerSecAvg, itemsPerSecNow, remaining));
				
				last = done;
				lastTime = System.currentTimeMillis();
			}
		}
		
		log.error(String.format("%,d tasks completed after %s.", ttlFiles, DurationFormatUtils.formatDuration(System.currentTimeMillis() - start, "HH:mm:ss.S")));
		
		log.info("done.");
	}
	
	public String getHostName(Tbl t) throws URISyntaxException {
		URI uri = new URI(t.getTblContext().getUrl());
		
		String host = uri.getHost();
		
		return host;
	}
}
