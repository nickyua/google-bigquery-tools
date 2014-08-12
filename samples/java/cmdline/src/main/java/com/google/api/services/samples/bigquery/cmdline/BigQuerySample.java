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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeRequestUrl;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Key;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.Bigquery.Projects;
import com.google.api.services.bigquery.Bigquery.Tabledata;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.ProjectList;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableCell;
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
import java.io.InputStream;
import java.io.Reader;
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
 */
public class BigQuerySample {

  private static final int SLEEP_MILLIS = 5000;
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();
  private static final HttpTransport TRANSPORT = new NetHttpTransport();

  // CHANGE ME!
  private static final String CLIENTSECRETS_LOCATION = "/path/to/your/client_secret.json";
  private static final List<String> SCOPES = Arrays.asList(BigqueryScopes.BIGQUERY);
  private static final String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";

  static GoogleClientSecrets clientSecrets = loadClientSecrets();
  private static GoogleAuthorizationCodeFlow flow = null;

  /** Directory to store user credentials. */
  private static final java.io.File DATA_STORE_DIR =
      new java.io.File(System.getProperty("user.home"), ".store/bq_cmd_sample");

  /**
   * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
   * globally shared instance across your application.
   */
  private static FileDataStoreFactory dataStoreFactory;


  /** Authorizes the installed application to access user's protected data. */
  private static Credential authorize() throws IOException {
    if (clientSecrets.getDetails().getClientId().startsWith("Enter")
        || clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
      System.out.println("Enter Client ID and Secret from https://code.google.com/apis/console/ "
          + "into oauth2-cmdline-sample/src/main/resources/client_secrets.json");
      System.exit(1);
    }
    dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);
    // set up authorization code flow
    GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
        TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES).setDataStoreFactory(
        dataStoreFactory).build();
    // authorize
    return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
  }


  public static Bigquery createAuthorizedClient() throws IOException {

    Credential credential = authorize();
    return new Bigquery(TRANSPORT, JSON_FACTORY, credential);
  }

  /**
   * Helper to load client ID/Secret from file.
   *
   * @return a GoogleClientSecrets object based on a clientsecrets.json
   */
  private static GoogleClientSecrets loadClientSecrets() {
    try {
      InputStream inputStream = new FileInputStream(CLIENTSECRETS_LOCATION);
        Reader reader =
          new InputStreamReader(inputStream);
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(new JacksonFactory(),
                reader);
        return clientSecrets;
    } catch (Exception e) {
      System.out.println("Could not load client_secrets.json");
      e.printStackTrace();
    }
    return null;
  }


  @Parameters(separators = "=", commandDescription = "List projects")
  static class CommandLsProjects implements Command {

    @Override
    public void run(Bigquery bigquery) throws IOException {
      Bigquery.Projects.List projectListRequest = bigquery.projects().list();
      ProjectList projectList = projectListRequest.execute();

      if (projectList.getProjects() != null) {
        List<ProjectList.Projects> projects = projectList.getProjects();
        System.out.println("Project list:");

        for (ProjectList.Projects project : projects) {
          System.out.format("%s\n", project.getFriendlyName());
        }
      }
    }
  }

  @Parameters(separators = "=", commandDescription = "List datasets")
  static class CommandLsDatasets implements Command {

    @Parameter(names = {"--projectId"}, required = true)
    public String projectId;

    @Parameter(names = {"--maxResults"})
    private long maxResults = 2L;

    @Override
    public void run(Bigquery bigquery) throws IOException {
      Datasets.List datasetRequest = bigquery.datasets().list(projectId);
      DatasetList datasetList = datasetRequest.setMaxResults(maxResults).execute();
      if (datasetList.getDatasets() != null) {
        List<DatasetList.Datasets> datasets = datasetList.getDatasets();
        System.out.println("Available datasets\n----------------");
        System.out.println(datasets.toString());
        for (DatasetList.Datasets dataset : datasets) {
          System.out.format("%s\n", dataset.getDatasetReference().getDatasetId());
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
      if (tableList.getTables() != null) {

        List<TableList.Tables> tables = tableList.getTables();
        System.out.println("Tables list:");
        for (TableList.Tables table : tables) {
          System.out.format("%s\n", table.getId());
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

    @Parameter(names = {"--csvFile"}, validateWith = GCSValidator.class, required = true)
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
      JSON_FACTORY.createJsonParser(new FileInputStream(schemaFile))
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

      Insert insertReq = bigquery.jobs().insert(projectId, insertJob);
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

      Insert insert = bigquery.jobs().insert(projectId, job);
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

  static class CommandHelp {

    public void run(JCommander jcom) {
      jcom.usage();
      System.exit(1);
    }
  }

  public static void main(String[] args) throws Exception {
    GlobalFlags global = new GlobalFlags();

    Bigquery bigquery = createAuthorizedClient();

    JCommander jcom = new JCommander(global);
    CommandLsProjects lsp = new CommandLsProjects();
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

    CommandHelp help = new CommandHelp();
    jcom.addCommand("help", help);

    jcom.parse(args);
    String parsedCommand = jcom.getParsedCommand();

    try {
      if ("lsp".equals(parsedCommand)) {
        lsp.run(bigquery);
      } else if ("lsd".equals(parsedCommand)) {
        lsd.run(bigquery);
      } else if ("lst".equals(parsedCommand)) {
        lst.run(bigquery);
      } else if ("load".equals(parsedCommand)) {
        load.run(bigquery);
      } else if ("query".equals(parsedCommand)) {
        query.run(bigquery);
      } else if ("squery".equals(parsedCommand)) {
        squery.run(bigquery);
      } else {
        help.run(jcom);
      }
    } catch (GoogleJsonResponseException ex) {
      System.err.println(ex.getMessage());
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
    println("job status: " + job.getStatus().getState());
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
      for (TableCell cell : row.getF()) {
        print("%-20s", ellipsize(cell.getV().toString()));
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
  public static class GCSValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
      if (value == null || value.isEmpty() || !value.startsWith("gs://")) {
        throw new ParameterException("--" + name + " must refer to a gs:// path");
      }
    }
  }
}
