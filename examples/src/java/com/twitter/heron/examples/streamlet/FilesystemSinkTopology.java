//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.examples.streamlet;

import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.examples.streamlet.utils.StreamletUtils;
import com.twitter.heron.streamlet.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;

public class FilesystemSinkTopology {
    private static class FilesystemSink<T> implements Sink<T> {
        private static final long serialVersionUID = -96514621878356224L;
        private Path tempFilePath;
        private File tempFile;

        FilesystemSink(File f) {
            this.tempFile = f;
        }

        public void setup(Context context) {
            this.tempFilePath = Paths.get(tempFile.toURI());
        }

        public void put(T element) {
            byte[] bytes = String.format("%s\n", element.toString()).getBytes();

            try {
                Files.write(tempFilePath, bytes, StandardOpenOption.APPEND);
                System.out.println(
                        String.format("Wrote %s to %s", new String(bytes), tempFilePath.toAbsolutePath())
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void cleanup() {
        }
    }

    public static void main(String[] args) throws Exception {
        Builder processingGraphBuilder = Builder.createBuilder();

        File f = File.createTempFile("filesystem-sink-example", ".tmp");
        System.out.println(String.format("Ready to write to file %s", f.getAbsolutePath()));

        processingGraphBuilder.newSource(() -> {
                    Utils.sleep(500);
                    return ThreadLocalRandom.current().nextInt(100);
                })
                .toSink(new FilesystemSink<>(f));

        Config config = new Config();

        String topologyName = StreamletUtils.getTopologyName(args);

        new Runner().run(topologyName, config, processingGraphBuilder);
    }
}
