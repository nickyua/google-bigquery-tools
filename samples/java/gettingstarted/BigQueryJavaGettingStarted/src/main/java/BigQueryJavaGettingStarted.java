/*
 * Copyright (c) 2012 Google Inc.
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

import com.google.api.client.auth.oauth2.draft10.AccessTokenResponse;
import com.google.api.client.googleapis.auth.oauth2.draft10.GoogleAccessProtectedResource;
import com.google.api.client.googleapis.auth.oauth2.draft10.GoogleAccessTokenRequest.GoogleAuthorizationCodeGrant;
import com.google.api.client.googleapis.auth.oauth2.draft10.GoogleAuthorizationRequestUrl;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.json.JsonHttpRequest;
import com.google.api.client.http.json.JsonHttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetListDatasets;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableRowF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Sample class demonstrating how to get started with BigQuery
 * and the Google Java API client libraries.
 */
public class BigQueryJavaGettingStarted {

  /////////////////////////
  // USER GENERATED FILES: you must fill in values specific to your application
  // 
  // Visit the Google API Console to create a Project and generate an
  // OAuth 2.0 Client ID and Secret (http://code.google.com/apis/console)
  /////////////////////////
  private static final String CLIENT_ID = "686269325431.apps.googleusercontent.com";
  private static final String CLIENT_SECRET = "1WpZoD3XYcn0fl_i9UMokcCv";
  private static final String PROJECT_ID = "686269325431";

  // Static variables for API scope, callback URI, and HTTP/JSON functions
  private static final String SCOPE = "https://www.googleapis.com/auth/bigquery";
  private static final String CALLBACK_URL = "urn:ietf:wg:oauth:2.0:oob";
  private static final HttpTransport TRANSPORT = new NetHttpTransport();
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  /**
   * @param args
   * @throws IOException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    // Create a new BigQuery client authorized via OAuth 2.0 protocol
    Bigquery bigquery = createAuthorizedClient();

    // Print out available datasets to the console
    listDatasets(bigquery, "publicdata");

    // Start a Query Job
    String querySql = "SELECT TOP(word, 50), COUNT(*) FROM publicdata:samples.shakespeare";
    JobReference jobId = startQuery(bigquery, PROJECT_ID, querySql);

    // Poll for Query Results, return result output
    Job completedJob = checkQueryResults(bigquery, PROJECT_ID, jobId);

    // Return and display the results of the Query Job
    displayQueryResults(bigquery, PROJECT_ID, completedJob);

  }

  /**
   * Creates an authorized BigQuery client service using the OAuth 2.0 protocol
   * <p>
   * This method first creates a BigQuery authorization URL, then prompts the
   * user to visit this URL in a web browser to authorize access. The
   * application will wait for the user to paste the resulting authorization
   * code at the command line prompt.
   *
   * @return an authorized BigQuery client
   * @throws IOException
   */
  public static Bigquery createAuthorizedClient() throws IOException {

    // Generate the URL to which we will direct users
    String authorizeUrl = new GoogleAuthorizationRequestUrl(CLIENT_ID,
        CALLBACK_URL, SCOPE).build();
    System.out.println("Paste this URL into a web browser to authorize BigQuery Access:\n" + authorizeUrl);

    // Prompt for user to enter their authorization code
    System.out.println("Type the code you received here: ");
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String authorizationCode = in.readLine();

    // Generate an authorization URL - this will be shown to the user
    // so the client can obtain an access token
    AccessTokenResponse response = new GoogleAuthorizationCodeGrant(
        TRANSPORT, JSON_FACTORY, CLIENT_ID, CLIENT_SECRET,
        authorizationCode, CALLBACK_URL).execute();
    System.out.format("\naccessToken=%s\nrefreshToken=%s\n\n",
        response.accessToken, response.refreshToken);

    GoogleAccessProtectedResource accessProtectedResource;
    accessProtectedResource = new GoogleAccessProtectedResource(
        response.accessToken, TRANSPORT, JSON_FACTORY,
        CLIENT_ID, CLIENT_SECRET, response.refreshToken);

    Bigquery bigquery = Bigquery.builder(TRANSPORT, JSON_FACTORY)
        .setHttpRequestInitializer(accessProtectedResource)
        .setJsonHttpRequestInitializer(new JsonHttpRequestInitializer() {
          public void initialize(JsonHttpRequest request) {
            BigqueryRequest bigqueryRequest = (BigqueryRequest) request;
            bigqueryRequest.setPrettyPrint(true);
          }
        }).build();

    return bigquery;
  }

  /**
   * Display all BigQuery Datasets associated with a Project
   *
   * @param bigquery an authorized BigQuery client
   * @param projectId a string containing the current project ID
   * @throws IOException
   */
  public static void listDatasets(Bigquery bigquery, String projectId)
      throws IOException {
    Datasets.List datasetRequest = bigquery.datasets().list(projectId);
    DatasetList datasetList = datasetRequest.execute();
    if (datasetList.getDatasets() != null) {
      List<DatasetListDatasets> datasets = datasetList.getDatasets();
      System.out.println("Available datasets\n----------------");
      for (DatasetListDatasets dataset : datasets) {
        System.out.format("%s\n", dataset.getDatasetReference().getDatasetId());
      }
    }
  }

  /**
   * Creates a Query Job for a particular query on a dataset
   *
   * @param bigquery an authorized BigQuery client
   * @param projectId a String containing the current Project ID
   * @param querySql the actual query string
   * @return a reference to the inserted query Job
   * @throws IOException
   */
  public static JobReference startQuery(Bigquery bigquery, String projectId,
      String querySql) throws IOException {
    System.out.format("\nInserting Query Job: %s\n", querySql);

    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    config.setQuery(queryConfig);

    job.setConfiguration(config);
    queryConfig.setQuery(querySql);

    Insert insert = bigquery.jobs().insert(job);
    insert.setProjectId(projectId);
    JobReference jobId =  insert.execute().getJobReference();

    System.out.format("\nJob ID of Query Job is: %s\n", jobId.getJobId());

    return jobId;
  }

  /**
   * Polls the status of a BigQuery job, returns Job reference if "Done"
   *
   * @param bigquery an authorized BigQuery client
   * @param projectId a string containing the current project ID
   * @param jobId a reference to an inserted query Job
   * @return a reference to the completed Job
   * @throws IOException
   * @throws InterruptedException
   */
  private static Job checkQueryResults(Bigquery bigquery, String projectId, JobReference jobId)
      throws IOException, InterruptedException {
    // Variables to keep track of total query time
    long startTime = System.currentTimeMillis();
    long elapsedTime = 0;

    while (true) {
      Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
      elapsedTime = System.currentTimeMillis() - startTime;
      System.out.format("Job status (%dms) %s: %s\n", elapsedTime,
          jobId.getJobId(), pollJob.getStatus().getState());
      if (pollJob.getStatus().getState().equals("DONE")) {
        return pollJob;
      }
      // Pause execution for one second before polling job status again, to 
      // reduce unnecessary calls to the BigQUery API and lower overall
      // application bandwidth.
      Thread.sleep(1000);
    }
  }

  /**
   * Makes an API call to the BigQuery API
   *
   * @param bigquery an authorized BigQuery client
   * @param projectId a string containing the current project ID
   * @param completedJob to the completed Job 
   * @throws IOException
   */
  private static void displayQueryResults(Bigquery bigquery,
      String projectId, Job completedJob) throws IOException {
    GetQueryResultsResponse queryResult = bigquery.jobs()
        .getQueryResults(
            PROJECT_ID,completedJob
            .getJobReference()
            .getJobId()
            ).execute();
    List<TableRow> rows = queryResult.getRows();
    System.out.print("\nQuery Results:\n------------\n");
    for (TableRow row : rows) {
      for (TableRowF field : row.getF()){
        System.out.printf("%-20s", field.getV());
      }
      System.out.println();
    }
  }

}
