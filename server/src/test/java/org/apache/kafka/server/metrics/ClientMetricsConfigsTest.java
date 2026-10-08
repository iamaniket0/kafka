/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsConfigsTest {

    @Test
    public void testParseMatchingPatternsWithEqualsInRegex() {
        // Test case for KAFKA-21041: patterns with equals signs in the regex part should work
        List<String> patterns = List.of("client_id=foo=bar");
        
        // This should not throw an exception after the fix
        Map<String, Pattern> result = ClientMetricsConfigs.parseMatchingPatterns(patterns);
        
        // Verify the pattern was parsed correctly
        assertEquals(1, result.size());
        assertTrue(result.containsKey("client_id"));
        
        // The regex should be "foo=bar", not just "foo"
        Pattern pattern = result.get("client_id");
        assertEquals("foo=bar", pattern.pattern());
        
        // Test that the pattern matches correctly - use lookingAt() instead of matches() for partial matching
        assertTrue(pattern.matcher("foo=bar").find());
        assertTrue(pattern.matcher("foo=bar=baz").find()); // Should match strings with additional equals
    }

    @Test
    public void testParseMatchingPatternsWithMultipleEquals() {
        // Test multiple equals signs in the regex
        List<String> patterns = List.of("client_id=a=b=c=d");
        
        Map<String, Pattern> result = ClientMetricsConfigs.parseMatchingPatterns(patterns);
        
        assertEquals(1, result.size());
        assertTrue(result.containsKey("client_id"));
        
        // The regex should be "a=b=c=d"
        Pattern pattern = result.get("client_id");
        assertEquals("a=b=c=d", pattern.pattern());
    }

    @Test
    public void testParseMatchingPatternsNormalCase() {
        // Test normal case without equals in regex
        List<String> patterns = List.of("client_id=foo", "client_software_version=11.1.*");
        
        Map<String, Pattern> result = ClientMetricsConfigs.parseMatchingPatterns(patterns);
        
        assertEquals(2, result.size());
        assertTrue(result.containsKey("client_id"));
        assertTrue(result.containsKey("client_software_version"));
        
        // Verify patterns
        assertEquals("foo", result.get("client_id").pattern());
        assertEquals("11.1.*", result.get("client_software_version").pattern());
    }

    @Test
    public void testParseMatchingPatternsInvalidPattern() {
        // Test that patterns without equals still fail
        List<String> patterns = List.of("client_id_no_equals");
        
        assertThrows(InvalidConfigurationException.class, () -> {
            ClientMetricsConfigs.parseMatchingPatterns(patterns);
        });
    }

    @Test
    public void testParseMatchingPatternsEmptyPattern() {
        // Test empty pattern
        List<String> patterns = List.of("");
        
        assertThrows(InvalidConfigurationException.class, () -> {
            ClientMetricsConfigs.parseMatchingPatterns(patterns);
        });
    }

    @Test
    public void testParseMatchingPatternsOnlyEquals() {
        // Test pattern with only equals sign - this should fail because parameter name is empty
        List<String> patterns = List.of("=");
        
        assertThrows(InvalidConfigurationException.class, () -> {
            ClientMetricsConfigs.parseMatchingPatterns(patterns);
        });
    }

    @Test
    public void testParseMatchingPatternsWithSpaces() {
        // Test pattern with spaces around equals
        List<String> patterns = List.of("client_id = foo = bar");
        
        Map<String, Pattern> result = ClientMetricsConfigs.parseMatchingPatterns(patterns);
        
        assertEquals(1, result.size());
        assertTrue(result.containsKey("client_id"));
        
        // The regex should be "foo = bar" (with spaces)
        Pattern pattern = result.get("client_id");
        assertEquals("foo = bar", pattern.pattern());
    }

    @Test
    public void testKafka21041BugReproduction() {
        // This test reproduces the original bug from KAFKA-21041
        // Before the fix, this would throw InvalidConfigurationException
        // After the fix, it should work correctly
        
        // The original problem: "client_id=foo=bar" should be parsed as:
        // parameter: client_id, regex: foo=bar
        // But the old implementation using split("=") would produce 3 parts and fail
        
        List<String> patterns = List.of("client_id=foo=bar");
        
        // This should not throw an exception
        Map<String, Pattern> result = ClientMetricsConfigs.parseMatchingPatterns(patterns);
        
        assertEquals(1, result.size());
        assertTrue(result.containsKey("client_id"));
        assertEquals("foo=bar", result.get("client_id").pattern());
    }
}