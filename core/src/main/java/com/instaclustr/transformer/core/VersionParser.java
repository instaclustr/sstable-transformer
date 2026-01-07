/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.instaclustr.transformer.core;

import picocli.CommandLine;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;

/**
 * Parses information from git.properties file bundled into resulting JAR in order to read
 * build version, build time and git commit.
 */
public class VersionParser
{
    public static String[] parse() throws IOException
    {
        Enumeration<URL> resources = CommandLine.class.getClassLoader().getResources("git.properties");

        Optional<String> implementationVersion = Optional.empty();
        Optional<String> buildTime = Optional.empty();
        Optional<String> gitCommit = Optional.empty();

        while (resources.hasMoreElements())
        {
            final URL url = resources.nextElement();

            Properties properties = new Properties();
            try (InputStream is = url.openStream())
            {
                properties.load(is);
            }

            if (properties.getProperty("git.build.time") != null)
            {
                implementationVersion = Optional.ofNullable(properties.getProperty("git.build.version"));
                buildTime = Optional.ofNullable(properties.getProperty("git.build.time"));
                gitCommit = Optional.ofNullable(properties.getProperty("git.commit.id.full"));
            }
        }

        return new String[]{
                String.format("%s %s", "transformer", implementationVersion.orElse("development build")),
                String.format("Build time: %s", buildTime.orElse("unknown")),
                String.format("Git commit: %s", gitCommit.orElse("unknown")),
        };
    }
}
