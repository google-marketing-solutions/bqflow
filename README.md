**Disclaimer:** This is not an official Google product. There is absolutely NO
WARRANTY provided for using this code. **The code is Apache Licensed and CAN BE
fully modified, white labeled, and disassembled by your team.**

# BQFlow

This repository contains very lite modules and tools for use with advanced data
solutions. Specifically it implements the Extract and Load parts of an ETL,
allowing [BigQuery](https://cloud.google.com/bigquery) to be used for the Transform. Imagine all the data in an API
appearing as a table in BigQuery for you to query. And then imagine being able
to write back to the API using a query so you can change settings.

When you use this repository, restful APIs are turned into tables in BigQuery.
You can then write [SQL logic](https://cloud.google.com/bigquery/docs/reference/standard-sql/introduction#sql)
to manipulate and analyze those tables to either present in a dashboard, or
write back into another API call. See the [Wiki](../../wiki) for examples.

All [Google APIs](https://developers.google.com/apis-explorer) are supported,
our team specifically uses this for:

  1. [Google Ads](https://developers.google.com/google-ads/api/rest/overview)
  1. [Display Video](https://developers.google.com/display-video/api/reference/rest)
  1. [Campaign Manager](https://developers.google.com/doubleclick-advertisers/rel_notes)
  1. [Search Ads](https://developers.google.com/search-ads/v2/reference)
  1. [YouTube](https://developers.google.com/youtube/v3/docs)
  1. [Google Analytics](https://developers.google.com/analytics/devguides/reporting/core/v4/rest)

See the [Wiki](../../wiki) for how to call each. In addition reporting data helpers exist for:

  1. [CM360](/bqflow/task/cm_report.py)
  1. [DV360](/bqflow/task/dv_report.py)
  1. [GA360](/bqflow/task/ga_report.py)

## Install

```
git clone https://github.com/google/bqflow
python3 -m pip install -r requirements.txt
```

## Run A Workflow

A workflow is a JSON file that contains API endpoints and parameters. See the
[Wiki](../../wiki) for details examples and details on workflows. You may also
receive workflow JSON files from Google when collaborating on a project. The
following command will show you how to run a workflow:

```
python3 bqflow/run.py -h
```

## Run A Group Of Workflows

To execute multiple workflows in parallel, use the following command:

```
python3 bqflow/schedule_local.py -h
```

## VM Runner Script

To execute workflows on a schedule within a VM, follow [these instructions](https://cloud.google.com/compute/docs/instances/startup-scripts/linux):

  1. Create a [VM](https://cloud.google.com/compute). These are recommended settings:
     * **Series:** E2
     * **Machine Type:** e2-highmem-2
     * **Boot Disk Size:** 10GB is enough, all data is stored in memory.
     * **Boot Disk Image:** Debian GNU/Linux 11 (bullseye) or higher
     * **Service Account:** One you create (see below) or None, depending on setup.
     * **Firewall:** Leave unchecked, there is no need for HTTP/HTTPS.
  1. Log into the VM, the below step is optional if you get a warning message about logging in:
     * Make sure you have at least one [VPC Network](https://console.cloud.google.com/networking/networks/list).
     * Enable SSH/IAP Firewall rule for that network, rule is browser ssh compatible:
       ```
       gcloud compute --project=[PROJECT NAME] firewall-rules create allow-ingress-from-iap --direction=INGRESS --priority=1000 --network=default --action=ALLOW --rules=tcp:22,tcp:3389 --source-ranges=35.235.240.0/20
       ```
  1. Install BQFlow.
     * **Install Git:** `sudo apt-get install git`
     * **Install Pip:** `sudo apt-get install python3-pip`
     * **Install BQFlow:** `git clone https://github.com/google/bqflow`
     * **Install Requirments:** `python3 -m pip install -r bqflow/requirements.txt`
     * **Print These Instructions In VM:** `python3 bqflow/schedule_local.py -h`
     * **Create Workflow Directory And Add Workflows:** `mkdir workflows`
     * **Run Workflows Manually:** `python3 bqflow/schedule_local.py`
  1. Set up the startup script.
     * Log out of the VM.
     * Edit the VM and navigate to Management > Automation > Automation, and add:
       ```
       #!/bin/bash
       sudo -u [YOUR USERNAME] bash -c 'python3 ~/bqflow/schedule_local.py ~/workflows'
       shutdown -h +1
       ```
       Find [YOUR USERNAME] on the VM by running `echo $USER`.
  1. Set up the [schedule tab](https://console.cloud.google.com/compute/instances/instanceSchedules?&tab=instanceSchedules).

**NOTE:** To prevent the VM from shutting down when you log in you will have to
comment out the startup logic, save, and then log in.

## Drive Runner Script

To execute the workflows on a schedule from [Google Drive](https://www.google.com/drive/download/):

  1. Create a dedicated [Service](https://developers.google.com/workspace/guides/create-credentials#service-account) credential.
  1. Be sure to grant the service the [IAM Role](https://cloud.google.com/iam/docs/grant-role-console) **roles/bigquery.dataOwner** and **roles/bigquery.jobUser**.
  1. Create a VM, follow **STEP 2** under **VM Runner Script**, and choose the above service credential.
  1. STOP the VM, not delete, just stop.
  1. Select [SCOPES](https://developers.google.com/identity/protocols/oauth2/scopes) to the service account:
     * At minimum you will need:
        * https://www.googleapis.com/auth/bigquery
        * https://www.googleapis.com/auth/drive
     * For advertising products you should consider:
        * https://www.googleapis.com/auth/doubleclickbidmanager
        * https://www.googleapis.com/auth/doubleclicksearch
        * https://www.googleapis.com/auth/analytics
        * https://www.googleapis.com/auth/youtube
        * https://www.googleapis.com/auth/display-video
        * https://www.googleapis.com/auth/ddmconversions
        * https://www.googleapis.com/auth/dfareporting
        * https://www.googleapis.com/auth/dfatrafficking
        * https://www.googleapis.com/auth/analytics.readonly
        * https://www.googleapis.com/auth/adwords
        * https://www.googleapis.com/auth/adsdatahub
        * https://www.googleapis.com/auth/content
        * https://www.googleapis.com/auth/cloud-vision
  1. To apply all the scopes run the [following gcloud command](https://cloud.google.com/sdk/gcloud/reference/beta/compute/instances/set-scopes) from [Cloud Shell](https://cloud.google.com/shell)::
     ```
     gcloud beta compute instances set-scopes [VM NAME] --scopes='https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/doubleclickbidmanager,https://www.googleapis.com/auth/doubleclicksearch,https://www.googleapis.com/auth/analytics,https://www.googleapis.com/auth/youtube,https://www.googleapis.com/auth/display-video,https://www.googleapis.com/auth/ddmconversions,https://www.googleapis.com/auth/dfareporting,https://www.googleapis.com/auth/dfatrafficking,https://www.googleapis.com/auth/analytics.readonly,https://www.googleapis.com/auth/adwords,https://www.googleapis.com/auth/adsdatahub,https://www.googleapis.com/auth/content,https://www.googleapis.com/auth/cloud-vision' --zone=[ZONE] --service-account=[SERVICE CREDENTIAL EMAIL]`
     ```
  1. Set up the startup script.
     * Edit the VM and navigate to Management > Automation > Automation, and add:
       ```
       #!/bin/bash
       sudo -u [YOUR USERNAME] bash -c 'python3 ~/bqflow/schedule_drive.py [DRIVE FOLDER LINK] -s DEFAULT -p [CLOUD PROJECT ID]'
       shutdown -h +1
       ```
       Find [YOUR USERNAME] on the VM by running `echo $USER`.
  1. Set up the [schedule tab](https://console.cloud.google.com/compute/instances/instanceSchedules?&tab=instanceSchedules).
  1. Start adding workflows to your drive folder and sharing with the service email address from step one.
     * For security reasons workflows have to be in [DRIVE FOLDER LINK].
     * Edit JSON files from your machine using [Google Drive For Desktop](https://www.google.com/drive/download/).


## Authentication Credentials

BQFlow can be run with either [Service](https://developers.google.com/workspace/guides/create-credentials#service-account)
or [User](https://developers.google.com/workspace/guides/create-credentials#oauth-client-id)
credentials. Service credentials are ideal for most workflows however you have
the option to use either. Please follow [Google Cloud Security Best Practices](https://cloud.google.com/security/best-practices)
when handling credentials.

  1. For [Service](https://developers.google.com/workspace/guides/create-credentials#service-account) you have 2 options:
     * **Keyless**, provision credentials and assign to VM, a key is never downloaded but all workflows must run as this service.
     * **JSON**, download the service keys to the VM (or equivalent) and use in combination with specific workflows.
     * Be sure to grant the service the [IAM Roles](https://cloud.google.com/iam/docs/grant-role-console) **roles/bigquery.dataOwner** and **roles/bigquery.jobUser**.
  1. For [User](https://developers.google.com/workspace/guides/create-credentials#oauth-client-id)
     * Run `python3 bqflow/auth.py -h` and follow instructions.

# Logs

  1. **For debugging** add the --verbose or -v parameter to any of the commands.
  2. **For production** add a log configuration to each workflow file. Change to __WRITE_TRUNCATE__ to replace the log file each time.
     ```
     {
       "log":{ "bigquery":{ "auth":"service", "dataset":"some_dataset", "table":"BQFlow_Log", "disposition":"WRITE_APPEND" }},
       "tasks":[...]
     }
     ```
     Logs are written after each workflow completes.  The log table can be included in queries to ensure dashboards or API calls are up to date.


# FAQ

**Why does this exist?**
   1. Enables Google gTech to deliver solutions with 90% less code to maintain.
   1. Gives you the ability to clone your own version and own the code.
   1. Eliminates hundreds of custom connectors and maintenance.
   1. Moves all solution logic to SQL, which aligns with data scientists.

**Why BigQuery?**

  1. More accessibility, SQL is easier to learn and use than Python.
  1. Supports nested JSON structures required by most APIs.
  1. Has [hundreds of functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators) for manipulating data.
  1. Allows combining of tables (API endpoints).
  1. Can be connected to [dashboards](https://lookerstudio.google.com/).

**Does it have to run on a VM?**
  1. No, its just Python you can run it anywhere, including local machines and cloud functions.
  1. We chose a VM because there are no time limits, so workflows can run for hours if necessary.

**Why Restful APIS?**
  1. Well [documented endpoints](https://developers.google.com/apis-explorer) for each product.
  1. Consistent and universal [across all products](https://developers.google.com/discovery/v1/reference/apis), no client library differences.
  1. Less to maintain, yes BQFlow only has 1 connector for ALL GOOGLE APIs.

**Is it only Google APIs?**
  1. No, any API handler can be added, but our use case is Google.
  2. More details on how to extend in the [Wiki](../../wiki).

**Is it cloud agnostic?**
  1. Yes, its just Python code you can run it from anywhere in any cloud.
  1. No, it writes to and from BigQuery, which is a Google Cloud product.

**Is it a framework?**
   1. No, its a Python function for making API calls to and from BigQuery.
   1. Yes, there is a sample VM startup script you can use to run multiple jobs.
