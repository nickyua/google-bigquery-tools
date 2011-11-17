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
   $ sudo apt-get install maven2

2. A valid Google account and a project configured at
   https://code.google.com/apis/console/.

3. The "client ID" and "client secret" values for your project, available at
   the above URL (click on "API Access").

Installing and running
======================
1. Follow the instructions on
   http://code.google.com/p/google-bigquery-tools/source/checkout to check
   out a copy of the code.

2. "cd" to the directory that contains the code you downloaded.

3. Run "mvn validate compile" to verify that your Java environment is set up
   correctly.

4. Run "mvn assembly:assembly" to build a .jar file for reuse.

5. If you already have a client_secrets.json file, copy it to the current
   directory.  Otherwise, generate a template by running
   "java -jar output/bigquerysample-deploy.jar clientsecrets" and then edit
   it with your favorite editor and replace the underscores (_________) with
   the values from the API Console.  You can learn more about
   client_secrets.json at
   http://code.google.com/p/google-api-python-client/wiki/ClientSecrets.

6. Create authentication credentials by running "java -jar
   output/bigquerysample-deploy.jar auth" and follow the instructions.  This
   will create a ".bigqueryj.token" file in your home directory.

7. Verify that everything works by attempting to list your projects by running
   the "lsp" command: java -jar output/bigquerysample-deploy.jar lsp"

8. You should see a list of projects.  Run
   "java -jar output/bigquerysample-deploy.jar" to see a list of other
   options available to you.

9. Run the same command again, but configure logging so that you can observe
   the details of the HTTP transactions:
   java -Djava.util.logging.config.file=logging.properties \
     -jar output/bigquerysample-deploy.jar lsp

Working in IntelliJ
===================
Maven can generate an IntelliJ project file for you:

1. Run: mvn idea:idea
2. Open the bigquery-cmdline-sample.ipr project in IntelliJ.


