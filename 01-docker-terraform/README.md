## Homework
Homework week1
Dmitriy Belozerov

Q1
docker build --help
--rm


Q2
pip lidt
0.42.0


Q3
select count(1) from green_taxi_data where date_trunc('day', lpep_pickup_datetime) = '2019-09-18' and date_trunc('day', lpep_dropoff_datetime) = '2019-09-18' ; 

15612


Q4
select date_trunc('day', lpep_pickup_datetime) as date, sum(trip_distance) as td from green_taxi_data group by date order by td desc

26 sep 2019


Q5
select z."Borough", count (1) as rc
from green_taxi_data g
join zones z on g."PULocationID" = z."LocationID"
where date_trunc('day', g.lpep_pickup_datetime)='2019-09-18'
group by z."Borough"
order by rc desc
limit 3;

Manhaaten, Brooklyn, Queens


Q6
select g."DOLocationID", g.tip_amount from green_taxi_data g
join zones z on g."PULocationID" = z."LocationID"
where z."Zone" = 'Astoria' and date_trunc('month', g.lpep_pickup_datetime)='2019-09-01'
order by g.tip_amount desc
limit 5;

zone 132 which is JFK airport



## Q7 Terraform apply


Terraform used the selected providers to generate the following execution plan.
Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be created
  + resource "google_bigquery_dataset" "demo_dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "demo_dataset"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = (known after apply)
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "europe-west1"
      + max_time_travel_hours      = (known after apply)
      + project                    = "static-mediator-411916"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = (known after apply)
    }

  # google_storage_bucket.demo-bucket will be created
  + resource "google_storage_bucket" "demo-bucket" {
      + effective_labels            = (known after apply)
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EUROPE-WEST1"
      + name                        = "static-mediator-411916-terra-bucket"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = (known after apply)
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "AbortIncompleteMultipartUpload"
            }
          + condition {
              + age                   = 30
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_storage_bucket.demo-bucket: Creation complete after 1s [id=static-mediator-411916-terra-bucket]
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/static-mediator-411916/datasets/demo_dataset]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
