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
package com.twitter.heron.apiserver.resources;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import javax.activation.MimetypesFileTypeMap;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.eclipse.jetty.util.StringUtil;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.heron.apiserver.utils.FileHelper;
import com.twitter.heron.apiserver.utils.Logging;
import com.twitter.heron.apiserver.utils.Utils;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;

/**
 * Endpoints that allows the API server to become a file server
 */
@Path("/file")
public class FileResource extends HeronResource {
  private static final Logger LOG = LoggerFactory.getLogger(FileResource.class);

  private static final String FILE_SYSTEM_DIRECTORY
      = "heron.apiserver.http.file.system.directory";

  private static final String DOWNLOAD_HOSTNAME_OVERRIDE
      = "heron.apiserver.http.download.hostname";

  private static InetAddress ip;
  private static String hostname;

  static {
    try {
      ip = InetAddress.getLocalHost();
      hostname = ip.getHostName();
    } catch (UnknownHostException e) {
      LOG.info("Failed to resolve IP address of localhost");
    }
  }

/**
 * Endpoints for artifacts upload
 */
  @POST
  @Path("/upload")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  public Response uploadFile(
      @FormDataParam("file") InputStream uploadedInputStream,
      @FormDataParam("file") FormDataContentDisposition fileDetail) {

    Config config = createConfig();

    if (uploadedInputStream == null) {
      String msg = "input stream is null";
      LOG.error(msg);
      return Response.status(Response.Status.BAD_REQUEST)
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(msg)).build();
    }
    if (fileDetail == null) {
      String msg = "form data content disposition is null";
      LOG.error(msg);
      return Response.status(Response.Status.BAD_REQUEST)
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(msg)).build();
    }

    String uploadDir = config.getStringValue(FILE_SYSTEM_DIRECTORY);

    final String fileName = UUID.randomUUID() + "-" + fileDetail.getFileName();

    final String uploadedFileLocation
        = uploadDir + "/" + fileName;

    // save it
    try {
      FileHelper.writeToFile(uploadedInputStream, uploadedFileLocation);
    } catch (IOException e) {
      LOG.error("error uploading file {}", fileDetail.getFileName(), e);
      return Response.serverError()
          .type(MediaType.APPLICATION_JSON)
          .entity(Utils.createMessage(e.getMessage()))
          .build();
    }

    String uri = String.format("http://%s:%s/api/v1/file/download/%s",
        getHostNameOrIP(), getPort(), fileName);

    return Response.status(Response.Status.OK).entity(uri).build();
  }

/**
 * Endpoints for artifacts download
 */
  @GET
  @Path("/download/{file}")
  public Response downloadFile(final @PathParam("file") String file) {
    Config config = createConfig();
    String uploadDir = config.getStringValue(FILE_SYSTEM_DIRECTORY);
    String filePath = uploadDir + "/" + file;
    if (!new File(filePath).exists()) {
      LOG.debug("Download request file " + file + " doesn't exist at " + uploadDir);
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    String mimeType = new MimetypesFileTypeMap().getContentType(file);
    Response.ResponseBuilder rb = Response.ok(file, mimeType);
    rb.header("content-disposition", "attachment; filename = "
        + file);
    return rb.build();
  }

  /**
   * Endpoint for downloading Heron Core
   */
  @GET
  @Path("/download/core")
  public Response downloadHeronCore() {
    String corePath = getHeronCorePackagePath();
    File file = new File(corePath);
    if (!file.exists()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    String mimeType = new MimetypesFileTypeMap().getContentType(file);
    Response.ResponseBuilder rb = Response.ok(file, mimeType);
    rb.header("content-disposition", "attachment; filename = "
        + file.getName());
    return rb.build();
  }

  private Config createConfig() {
    final Config.Builder builder = Config.newBuilder().putAll(getBaseConfiguration());
    builder.put(Key.VERBOSE, Logging.isVerbose());
    return Config.toLocalMode(builder.build());
  }

  private String getHostNameOrIP() {
    // Override hostname if provided in flags
    if (StringUtil.isNotBlank(getDownloadHostName())) {
      return getDownloadHostName();
    } else if (StringUtil.isNotBlank(hostname)) {
      return hostname;
    } else if (ip != null && StringUtil.isNotBlank(ip.toString())) {
      return ip.toString();
    }
    return "";
  }

}
