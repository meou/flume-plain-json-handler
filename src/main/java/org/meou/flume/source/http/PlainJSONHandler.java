/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.meou.flume.source.http;

import com.google.gson.JsonParser;
import com.google.gson.JsonParseException;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PlainJSONHandler for HTTPSource that accepts json-based http body.
 *
 * This handler throws exception if the deserialization fails because of bad
 * format or any other reason.
 */

public class PlainJSONHandler implements HTTPSourceHandler {
  
  private static final Logger LOG = 
    LoggerFactory.getLogger(PlainJSONHandler.class);
  private static JsonParser parser = new JsonParser();

  @Override
  public List<Event> getEvents(HttpServletRequest request) throws Exception {

    BufferedReader reader = request.getReader();
    List<Event> eventList = new ArrayList<Event>(0);
    String line = null;
    while ((line = reader.readLine()) != null) {
      try {
        parser.parse(line);
      } catch (JsonParseException ex) {
        throw new HTTPBadRequestException("HTTP body line # " 
          + Integer.toString(eventList.size()) 
          + " is not a valid JSON object.", ex);
      }
      Event event = new JSONEvent();
      event.setBody(line.getBytes());
      eventList.add(event);
    }
    if (eventList.isEmpty()) {
      LOG.debug("No event is generated.");
    }
    return eventList;
  }

  @Override
  public void configure(Context context) {
  }
}
