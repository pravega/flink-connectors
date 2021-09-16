/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.connectors.flink.utils;

import io.pravega.connectors.flink.dynamic.table.FlinkPravegaDynamicTableITCase;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestUtils {
    // Linux uses ports from range 32768 - 61000.
    private static final int BASE_PORT = 32768;
    private static final int MAX_PORT_COUNT = 28232;
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(1);

    /**
     * A helper method to get a random free port.
     *
     * @return free port.
     */
    public static int getAvailableListenPort() {
        for (int i = 0; i < MAX_PORT_COUNT; i++) {
            int candidatePort = BASE_PORT + NEXT_PORT.getAndIncrement() % MAX_PORT_COUNT;
            try {
                ServerSocket serverSocket = new ServerSocket(candidatePort);
                serverSocket.close();
                return candidatePort;
            } catch (IOException e) {
                // Do nothing. Try another port.
            }
        }
        throw new IllegalStateException(
                String.format("Could not assign port in range %d - %d", BASE_PORT, MAX_PORT_COUNT + BASE_PORT));
    }

    /**
     * A helper method reading the content from a resource file.
     *
     * @param resource file name, in the resources directory
     * @return the content of the file
     * @throws IOException exception
     */
    public static List<String> readLines(String resource) throws IOException {
        final URL url = FlinkPravegaDynamicTableITCase.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }
}
