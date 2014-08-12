BigQuerySample - sample code that uses the Google BigQuery API from Java.
Copyright 2011 Google Inc.
http://code.google.com/p/google-bigquery-tools/

About
=====
This directory contains sample Java code Google is releasing as open source to
demonstrate how to use the Google BigQuery API.

The code in this directory is primarily an educational aid for customers who
want to use the Google BigQuery APIs from Java.

If you are looking for a reusable command line tool, please use the supported
"bq" tool available at http://code.google.com/p/google-bigquery-tools/ rather
than this code.

This code is unsupported.

Prerequisites
=============
1. Building this code requires Maven 2.0 or higher and a recent Java
   development kit (JDK).

   If you are on a Debian-based Linux distribution (such as Ubuntu), the
   following command should get Maven installed for you:

   ```
   $ sudo apt-get install maven2
   ```

2. A valid Google account, and a project configured at
   [https://console.developers.google.com](https://console.developers.google.com), with
   the BigQuery API enabled for that project as described [here](https://developers.google.com/bigquery/bigquery-api-quickstart).


Installing and running
======================

1. Follow the instructions on
   http://code.google.com/p/google-bigquery-tools/source/checkout to check
   out a copy of the code.

2. `cd` to the directory that contains the code you downloaded.

3. Run `mvn validate compile` to verify that your Java environment is set up
   correctly.

4. Generate a `client_secrets.json` file as described [here](https://developers.google.com/bigquery/bigquery-api-quickstart#client), or if you already have such a file, copy it to the current directory.  Then, edit the `CLIENTSECRETS_LOCATION` variable in the `BigQuerySample` class to point to that file's path. (Look for the "CHANGE ME" comment).

5. Run `mvn assembly:assembly` to build a .jar file for reuse.

6. Verify that everything works by attempting to list your projects by running the "lsd" command, substituting your project name or number for the 'xxxxx':

    ```
    java -jar output/bigquerysample-deploy.jar lsd --projectId xxxxx
    ```

7. You should see a list of projects.  Run
   "java -jar output/bigquerysample-deploy.jar" to see a list of other
   options available to you.

8. Run the same command again, but configure logging so that you can observe
   the details of the HTTP transactions:

     ```
     java -Djava.util.logging.config.file=logging.properties \
       -jar output/bigquerysample-deploy.jar lsp
     ```

Working in IntelliJ
===================

Maven can generate an IntelliJ project file for you:

1. Run: `mvn idea:idea`
2. Open the `bigquery-cmdline-sample.ipr` project in IntelliJ.


