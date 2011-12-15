/*
 * Copyright (c) 2011 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.api.services.samples.bigquery.cmdline;

import com.google.api.client.auth.oauth2.draft10.AccessTokenResponse;
import com.google.api.client.googleapis.auth.oauth2.draft10.GoogleAccessProtectedResource;
import com.google.api.client.googleapis.auth.oauth2.draft10.GoogleAccessTokenRequest.GoogleAuthorizationCodeGrant;
import com.google.api.client.googleapis.auth.oauth2.draft10.GoogleAuthorizationRequestUrl;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.json.JsonHttpRequest;
import com.google.api.client.http.json.JsonHttpRequestInitializer;
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.client.util.Key;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.Bigquery.Projects;
import com.google.api.services.bigquery.Bigquery.Tabledata;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetListDatasets;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.ProjectList;
import com.google.api.services.bigquery.model.ProjectListProjects;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableListTables;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableRowF;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.validators.PositiveInteger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Demonstrates various Bigquery API calls using the Java API.
 *
 * This is sample code.  Not intended for production use.
 *
 * If you're looking for a command line client for using Bigquery, use the "bq" client available at
 * http://code.google.com/p/google-bigquery-tools/.
 */
public class BigQuerySample {

  private static final String SCOPE = "https://www.googleapis.com/auth/bigquery";
  private static final String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";
  private static final int SLEEP_MILLIS = 5000;
  private static final JacksonFactory JACKSON = new JacksonFactory();

  @Parameters(separators = "=", commandDescription = "List projects")
  static class CommandLsProjects implements Command {

    @Override
    public void run(Bigquery bigquery) throws IOException {
      Projects.List listReq = bigquery.projects().list();
      ProjectList projectList = listReq.setMaxResults(2L).execute();
      while (projectList.getProjects() != null) {
        List<ProjectListProjects> projects = projectList.getProjects();
        for (ProjectListProjects project : projects) {
          println(project.getId() + ": " + project.getFriendlyName());
        }
        if (projectList.getNextPageToken() != null) {
          projectList = listReq.setPageToken(projectList.getNextPageToken()).execute();
        } else {
          break;
        }
      }
    }
  }

  @Parameters(separators = "=", commandDescription = "List datasets")
  static class CommandLsDatasets implements Command {

    @Parameter(names = {"--projectId"}, required = true)
    public String projectId;

    @Override
    public void run(Bigquery bigquery) throws IOException {
      Datasets.List listReq = bigquery.datasets().list(projectId);
      DatasetList datasetList = listReq.setMaxResults(2L).execute();
      while (datasetList.getDatasets() != null) {
        List<DatasetListDatasets> datasets = datasetList.getDatasets();
        for (DatasetListDatasets dataset : datasets) {
          println(dataset.getDatasetReference().toString());
        }
        if (datasetList.getNextPageToken() != null) {
          datasetList = listReq.setPageToken(datasetList.getNextPageToken()).execute();
        } else {
          break;
        }
      }
    }
  }

  @Parameters(separators = "=", commandDescription = "List tables")
  static class CommandLsTables implements Command {

    @Parameter(names = {"--projectId"}, required = true)
    public String projectId;

    @Parameter(names = {"--datasetId"}, required = true)
    public String datasetId;

    @Parameter(names = {"--maxResults"})
    private long maxResults = 2L;

    @Override
    public void run(Bigquery bigquery) throws IOException {
      Tables.List listReq = bigquery.tables().list(projectId, datasetId);
      TableList tableList = listReq.setMaxResults(maxResults).execute();
      while (tableList.getTables() != null) {
        List<TableListTables> tables = tableList.getTables();
        String format = "Table %s: %s";
        for (TableListTables table : tables) {
          println(format, table.getId(), table.getFriendlyName());
        }
        if (tableList.getNextPageToken() != null) {
          tableList = listReq.setPageToken(tableList.getNextPageToken()).execute();
        } else {
          break;
        }
      }
    }
  }

  @Parameters(separators = "=",
      commandDescription = "Create a new table and populate it with data from a CSV file on "
          + "Google Storage")
  static class CommandLoad implements Command {

    @Parameter(names = {"--projectId"}, required = true)
    public String projectId;

    @Parameter(names = {"--datasetId"}, required = true)
    public String datasetId;

    @Parameter(names = {"--tableId"}, required = true)
    public String tableId;

    @Parameter(names = {"--csvFile"}, validateWith = BigstoreValidator.class, required = true)
    public String csvFile;

    @Parameter(names = "--schemaFile", validateWith = FilenameValidator.class, required = true)
    public File schemaFile;

    @Parameter(names = "--skipLeadingRows", validateWith = PositiveInteger.class)
    public Integer skipLeadingRows;

    @Parameter(names = "--maxBadRecords", validateWith = PositiveInteger.class)
    public Integer maxBadRecords = 100;

    @Parameter(names = "--encoding", description = "'UTF-8' or 'ISO-8859-1'")
    public String encoding;

    @Override
    public void run(Bigquery bigquery) throws IOException {
      Job insertJob = new Job();
      insertJob.setJobReference(new JobReference().setProjectId(projectId));

      TableSchema schema = new TableSchema();
      schema.setFields(new ArrayList<TableFieldSchema>());
      JACKSON.createJsonParser(new FileInputStream(schemaFile))
          .parseArrayAndClose(schema.getFields(), TableFieldSchema.class, null);
      JobConfiguration configuration = new JobConfiguration();
      JobConfigurationLoad load = new JobConfigurationLoad();
      load.setSchema(schema);
      load.setCreateDisposition("CREATE_IF_NEEDED");
      TableReference destinationTable = new TableReference();
      destinationTable.setProjectId(projectId);
      destinationTable.setDatasetId(datasetId);
      destinationTable.setTableId(tableId);
      load.setDestinationTable(destinationTable);
      load.setSourceUris(Arrays.asList(csvFile));
      if (skipLeadingRows != null) {
        load.setSkipLeadingRows(skipLeadingRows);
      }
      if (maxBadRecords != null) {
        load.setMaxBadRecords(maxBadRecords);
      }
      if (encoding != null) {
        load.setEncoding(encoding);
      }
      configuration.setLoad(load);
      insertJob.setConfiguration(configuration);

      Insert insertReq = bigquery.jobs().insert(insertJob);
      insertReq.setProjectId(projectId);
      println("Starting load job.");
      Job job = insertReq.execute();
      if (isJobRunning(job)) {
        Job doneJob = waitForJob(bigquery, projectId, job.getJobReference());
        println("Done: " + doneJob.toString());
      } else {
        println("Error: " + job.toString());
      }
    }
  }

  @Parameters(separators = "=", commandDescription = "Queries a table (async)")
  static class CommandQuery implements Command {

    @Parameter(names = {"--projectId"}, required = true)
    public String projectId;

    @Parameter(names = {"--datasetId"}, required = true)
    public String datasetId;

    @Parameter(names = {"--query"}, required = true)
    public String query;

    @Parameter(names = {"--maxResults"})
    public Long maxResults = 10L;

    @Override
    public void run(Bigquery bigquery) throws IOException {
      Job job = new Job();
      JobConfiguration config = new JobConfiguration();
      JobConfigurationQuery queryConfig = new JobConfigurationQuery();
      config.setQuery(queryConfig);

      DatasetReference defaultDataset = new DatasetReference()
          .setDatasetId(datasetId)
          .setProjectId(projectId);
      queryConfig.setDefaultDataset(defaultDataset);
      job.setConfiguration(config);
      queryConfig.setQuery(query);

      Insert insert = bigquery.jobs().insert(job);
      insert.setProjectId(projectId);
      Job jobDone = waitForJob(bigquery, projectId, insert.execute().getJobReference());

      Tabledata.List listReq = bigquery.tabledata().list(projectId,
          jobDone.getConfiguration().getQuery().getDestinationTable().getDatasetId(),
          jobDone.getConfiguration().getQuery().getDestinationTable().getTableId());
      listReq.setMaxResults(maxResults);
      TableDataList tableDataList = listReq.execute();

      int rowsFetched = 0;
      while (rowsFetched < tableDataList.getTotalRows()) {
        listReq.setStartIndex(BigInteger.valueOf(rowsFetched));
        tableDataList = listReq.execute();

        printRows(null, tableDataList.getRows());
        rowsFetched += tableDataList.getRows().size();
      }
    }
  }

  @Parameters(separators = "=", commandDescription = "Queries a table (sync)")
  static class CommandSyncQuery implements Command {

    @Parameter(names = {"--projectId"}, required = true)
    public String projectId;

    @Parameter(names = {"--datasetId"}, required = true)
    public String datasetId;

    @Parameter(names = {"--query"}, required = true)
    public String query;

    @Parameter(names = {"--maxResults"})
    public Long maxResults = 10L;

    @Override
    public void run(Bigquery bigquery) throws IOException {
      QueryRequest request = new QueryRequest();
      DatasetReference defaultDataset = new DatasetReference()
          .setDatasetId(datasetId)
          .setProjectId(projectId);
      request.setDefaultDataset(defaultDataset);
      request.setMaxResults(maxResults);
      request.setQuery(query);
      request.setTimeoutMs(0L);
      QueryResponse response = bigquery.jobs().query(projectId, request).execute();

      // The first set of results will come from the QueryResponse if the job completed; otherwise,
      // we get it by calling getQueryResults().
      int rowsFetched = 0;
      BigInteger totalRows;
      if (response.getJobComplete()) {
        // job is complete, fetch first
        printRows(response.getSchema(), response.getRows());
        rowsFetched += response.getRows().size();
        totalRows = response.getTotalRows();
      } else {
        // job is not complete, wait for rest of query results
        GetQueryResults resultsReq =
            bigquery.jobs().getQueryResults(projectId, response.getJobReference().getJobId());
        resultsReq.setTimeoutMs(5000L);
        resultsReq.setMaxResults(maxResults);
        GetQueryResultsResponse queryResults = waitForQueryResults(resultsReq);

        // query is done
        printRows(response.getSchema(), queryResults.getRows());
        rowsFetched += queryResults.getRows().size();
        totalRows = queryResults.getTotalRows();
      }

      // Fetch any additional rows
      if (BigInteger.valueOf(rowsFetched).compareTo(totalRows) < 0) {
        GetQueryResults pagination =
            bigquery.jobs().getQueryResults(projectId, response.getJobReference().getJobId());
        pagination.setStartIndex(BigInteger.valueOf(rowsFetched));
        GetQueryResultsResponse page = pagination.execute();
        Preconditions.checkState(page.getJobComplete());

        rowsFetched += page.getRows().size();
        printRows(response.getSchema(), page.getRows());
      }
    }

    private GetQueryResultsResponse waitForQueryResults(GetQueryResults req)
        throws IOException {
      GetQueryResultsResponse response = req.execute();
      while (!response.getJobComplete()) {
        sleep();
        response = req.execute();
      }
      return response;
    }
  }

  @Parameters(separators = "=", commandDescription = "Global flags")
  static class GlobalFlags {

    @Parameter(names = "--credentialsFile")
    public String credentialsFile = System.getProperty("user.home") + "/.bigqueryj.token";

    @Parameter(names = "--clientSecretsFile")
    public String clientSecretsFile = "client_secrets.json";

    @Parameter(names = "--altServer")
    public String altServer;
  }

  @Parameters(separators = "=", commandDescription = "Authenticate")
  static class CommandAuth {
    // intentionally empty
  }

  @Parameters(separators = "=", commandDescription = "Generates a sample client_secrets.json file")
  static class CommandClientSecrets {

    public void run(String clientSecretsFilename) throws IOException {
      if (new File(clientSecretsFilename).exists()) {
        throw new RuntimeException(clientSecretsFilename + " already exists.");
      }

      FileWriter writer = new FileWriter(clientSecretsFilename);
      writer.write("{\n"
          + "  \"installed\": {\n"
          + "    \"client_id\": \"________________.apps.googleusercontent.com\",\n"
          + "    \"client_secret\":\"________________\",\n"
          + "    \"redirect_uris\": [\"http://localhost\", \"urn:ietf:oauth:2.0:oob\"],\n"
          + "    \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
          + "    \"token_uri\": \"https://accounts.google.com/o/oauth2/token\"\n"
          + "  }\n"
          + "}");
      writer.close();
      println("A client_secrets.json file has been created in the current directory.  Open it in ");
      println("an editor and replace the client_id and client_secret values with those obtained ");
      println("from: https://code.google.com/apis/console");
    }
  }

  static class CommandHelp {

    public void run(JCommander jcom) {
      jcom.usage();
      System.exit(1);
    }
  }

  public static void main(String[] args) throws Exception {
    GlobalFlags global = new GlobalFlags();

    JCommander jcom = new JCommander(global);
    CommandLsProjects lsp = new CommandLsProjects();
    jcom.addCommand("auth", new CommandAuth());
    jcom.addCommand("lsp", lsp);
    CommandLsDatasets lsd = new CommandLsDatasets();
    jcom.addCommand("lsd", lsd);
    CommandLsTables lst = new CommandLsTables();
    jcom.addCommand("lst", lst);
    CommandLoad load = new CommandLoad();
    jcom.addCommand("load", load);
    CommandQuery query = new CommandQuery();
    jcom.addCommand("query", query);
    CommandSyncQuery squery = new CommandSyncQuery();
    jcom.addCommand("squery", squery);
    CommandClientSecrets clientSecretsCommand = new CommandClientSecrets();
    jcom.addCommand("clientsecrets", clientSecretsCommand);
    CommandHelp help = new CommandHelp();
    jcom.addCommand("help", help);

    jcom.parse(args);
    String parsedCommand = jcom.getParsedCommand();
    if ("clientsecrets".equals(parsedCommand)) {
      clientSecretsCommand.run(global.clientSecretsFile);
      System.exit(0);
    }

    ClientSecrets clientSecrets = parseClientSecrets(global);
    CredentialsStore credentials = new CredentialsStore(global.credentialsFile);
    BigqueryProvider provider = new BigqueryProvider(
        credentials,
        global.altServer,
        clientSecrets.clientId,
        clientSecrets.clientSecret);

    try {
      if ("auth".equals(parsedCommand)) {
        credentials.clear();
        provider.authorize();
      } else if ("lsp".equals(parsedCommand)) {
        lsp.run(provider.require());
      } else if ("lsd".equals(parsedCommand)) {
        lsd.run(provider.require());
      } else if ("lst".equals(parsedCommand)) {
        lst.run(provider.require());
      } else if ("load".equals(parsedCommand)) {
        load.run(provider.require());
      } else if ("query".equals(parsedCommand)) {
        query.run(provider.require());
      } else if ("squery".equals(parsedCommand)) {
        squery.run(provider.require());
      } else {
        help.run(jcom);
      }
    } catch (GoogleJsonResponseException ex) {
      System.err.println(ex.getMessage());
    }
  }

  /**
   * Constructs initialized BigQuery clients.
   */
  private static class BigqueryProvider {

    private CredentialsStore credentials;
    private String server;
    private final String clientId;
    private final String clientSecret;

    public BigqueryProvider(CredentialsStore credentials,
        String server,
        String clientId,
        String clientSecret) {
      this.credentials = credentials;
      this.server = server;
      this.clientId = clientId;
      this.clientSecret = clientSecret;
    }

    /**
     * Returns an intialized BigQuery client.  Will throw an exception if user has not
     * authenticated.
     */
    public Bigquery require() throws Exception {
      if (credentials.read() == null) {
        throw new RuntimeException(
            "You must run the 'auth' command to authenticate before running any other commands.");
      }
      return authorize();
    }

    /**
     * Returns an intialized BigQuery client.  It may prompt the user to authenticate.
     */
    public Bigquery authorize() throws Exception {
      NetHttpTransport transport = new NetHttpTransport();
      AccessTokenResponse creds = credentials.read();
      GoogleAccessProtectedResource accessProtectedResource;
      if (creds == null) {
        println("Acquiring new credentials.");
        String url = new GoogleAuthorizationRequestUrl(clientId, REDIRECT_URI, SCOPE).build();

        println("Go to this URL in your browser: " + url);
        println("Enter the authorization code and press [ENTER]: ");
        String authorizationCode = readLine();

        AccessTokenResponse response = new GoogleAuthorizationCodeGrant(transport, JACKSON,
            clientId, clientSecret, authorizationCode, REDIRECT_URI).execute();
        println("accessToken=%s\nrefreshToken=%s\n", response.accessToken,
            response.refreshToken);
        credentials.write(response);

        accessProtectedResource = new GoogleAccessProtectedResource(
            response.accessToken, transport, JACKSON, clientId,
            clientSecret, response.refreshToken);
      } else {
        accessProtectedResource = new GoogleAccessProtectedResource(creds.accessToken, transport,
            JACKSON, clientId, clientSecret, creds.refreshToken);
      }

      Bigquery bigquery = Bigquery.builder(transport, JACKSON)
          .setApplicationName("Google-BigQuerySample/1.0")
          .setHttpRequestInitializer(accessProtectedResource)
          .setJsonHttpRequestInitializer(new JsonHttpRequestInitializer() {
            @Override
            public void initialize(JsonHttpRequest request) {
              BigqueryRequest bigqueryRequest = (BigqueryRequest) request;
              bigqueryRequest.setPrettyPrint(true);
            }
          }).build();
      if (server != null) {
        bigquery.setBaseServer(server);
      }
      return bigquery;
    }

    private String readLine() throws IOException {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      return reader.readLine();
    }
  }

  /**
   * Serializes and deserializes the AccessTokenResponse file to disk.  This file contains the
   * user's access and refresh tokens.
   */
  private static class CredentialsStore {

    private final String credentials;

    public CredentialsStore(String credentials) {
      this.credentials = credentials;
    }

    public void clear() {
      new File(credentials).delete();
    }

    public AccessTokenResponse read() {
      File file = new File(credentials);
      if (!file.exists()) {
        return null;
      }
      try {
        JsonParser parser = JACKSON.createJsonParser(new FileReader(credentials));
        return parser.parseAndClose(AccessTokenResponse.class, null);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public void write(AccessTokenResponse response) {
      String payload = JACKSON.toString(response);
      try {
        FileWriter fileWriter = new FileWriter(credentials);
        fileWriter.write(payload);
        fileWriter.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Wraps the OAuth 2.0 client ID and client secret.
   */
  private static class ClientSecrets {

    @Key("client_id")
    String clientId;
    @Key("client_secret")
    String clientSecret;
  }

  private static ClientSecrets parseClientSecrets(GlobalFlags global) throws IOException {
    File clientSecretsFile = new File(global.clientSecretsFile);
    if (clientSecretsFile.exists()) {
      JsonParser parser = JACKSON.createJsonParser(new FileInputStream(clientSecretsFile));
      parser.skipToKey("installed");
      ClientSecrets clientSecrets = new ClientSecrets();
      parser.parse(clientSecrets, null);
      Preconditions.checkState(clientSecrets.clientId != null);
      Preconditions.checkState(clientSecrets.clientSecret != null);
      return clientSecrets;
    } else {
      throw new RuntimeException(
          "The --clientSecrets file is missing. Run 'clientsecrets' to get one.");
    }
  }

  private static Job waitForJob(Bigquery bigquery, String projectId, JobReference jobRef)
      throws IOException {
    while (true) {
      sleep();
      Job pollJob = bigquery.jobs().get(projectId, jobRef.getJobId()).execute();
      println("Waiting on job %s ... Current status: %s", jobRef.getJobId(),
          pollJob.getStatus().getState());
      if (!isJobRunning(pollJob)) {
        return pollJob;
      }
    }
  }

  private static void sleep() {
    try {
      Thread.sleep(SLEEP_MILLIS);
    } catch (InterruptedException e) {
      // ignore
    }
  }

  private static boolean isJobRunning(Job job) {
    return job.getStatus().getState().equals("RUNNING") ||
        job.getStatus().getState().equals("PENDING");
  }

  private static void printRows(@Nullable TableSchema schema, List<TableRow> rows) {
    if (schema != null) {
      for (TableFieldSchema field : schema.getFields()) {
        print("%-20s", ellipsize(field.getName()));
      }
      println();
    }
    for (TableRow row : rows) {
      for (TableRowF cell : row.getF()) {
        print("%-20s", ellipsize(cell.getV()));
      }
      println();
    }
  }

  private static String ellipsize(String text) {
    if (text.length() > 20) {
      text = text.substring(0, 17) + "...";
    }
    return text;
  }

  private static void println() {
    System.out.println();
  }

  private static void println(String line) {
    System.out.println(line);
  }

  private static void println(String format, Object... args) {
    System.out.println(String.format(format, args));
  }

  private static void print(String format, Object... args) {
    System.out.print(String.format(format, args));
  }

  /**
   * All "commands" implement this interface.
   */
  interface Command {

    void run(Bigquery bigquery) throws IOException;
  }

  /**
   * Validates that a filename is not null, empty, and that it exists.
   */
  public static class FilenameValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
      if (value == null || value.isEmpty() || !new File(value).exists()) {
        throw new ParameterException("--" + name + " must refer to an existing file.");
      }
    }
  }

  /**
   * Validates that a filename looks like a gs:// path.
   */
  public static class BigstoreValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
      if (value == null || value.isEmpty() || !value.startsWith("gs://")) {
        throw new ParameterException("--" + name + " must refer to a gs:// path");
      }
    }
  }
}
