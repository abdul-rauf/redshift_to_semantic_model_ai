## Step Outputs

This file stores **machine-readable outputs** for each step so later steps can read from here instead of re-running heavy discovery.

### Step 1 – Redshift metadata discovery

Expected shape (do not edit keys, only replace the value):

```json
{
  "step1_redshift_metadata": {
    "source": "C:\\\\Users\\\\admin2\\\\.cursor\\\\projects\\\\c-Users-admin2-Desktop-RedshiftToSemantic-V2\\\\agent-tools\\\\d9aa048c-c238-4dc5-b92f-cb49d63e2b66.txt",
    "format": "QueryResult",
    "description": "Full SVV_ALL_COLUMNS metadata (table_name, column_name, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale, ordinal_position) for all tables in REDSHIFT_TABLES within schema 'source'."
  },
  "step2_role_inference": {
    "schema_output": "rfrt",
    "tables": [
      {
        "table": "rfrt.dim_customer",
        "source": "source.ams_rem_crm_customer",
        "role": "dimension",
        "pk": ["customer_key_sk"],
        "businessKeys": ["contactid"],
        "fks": [],
        "measures": [],
        "attributes": [
          "oldid",
          "taxid",
          "versionstamp",
          "customertypeid",
          "type",
          "showindirectory",
          "publishinfoonline",
          "about",
          "sourceid",
          "currencyid",
          "parentid",
          "recordnumber",
          "meta_date_added",
          "meta_date_updated",
          "meta_record_status"
        ]
      },
      {
        "table": "rfrt.dim_individual",
        "source": "source.ams_rem_crm_individual",
        "role": "dimension",
        "pk": ["individual_key_sk"],
        "businessKeys": ["id"],
        "fks": [
          {
            "from": "customerid",
            "toTable": "rfrt.dim_customer",
            "toColumn": "customer_key_sk"
          }
        ],
        "measures": [],
        "attributes": [
          "suffix",
          "gender",
          "title",
          "deceased",
          "maidenname",
          "maritalstatus",
          "recordnumber",
          "oldid",
          "showindirectory",
          "publishinfoonline",
          "sourceid",
          "currencyid",
          "prefcommunicationtype",
          "createdon",
          "modifiedon",
          "isdeleted",
          "username",
          "accountstartdate",
          "salutation",
          "accountexpirationdate",
          "usercreatedon",
          "tags",
          "lastlogindate",
          "versionstamp",
          "prefix",
          "firstname",
          "middlename",
          "lastname",
          "lastname2",
          "preferredname",
          "meta_date_added",
          "meta_date_updated",
          "meta_record_status"
        ]
      },
      {
        "table": "rfrt.fact_registrations",
        "source": "source.ams_rem_purchase_allregistrations",
        "role": "fact",
        "pk": ["registration_key_sk"],
        "businessKeys": ["id", "unique_key"],
        "fks": [
          {
            "from": "customerid",
            "toTable": "rfrt.dim_customer",
            "toColumn": "customer_key_sk"
          }
        ],
        "measures": [],
        "attributes": [
          "createdon",
          "modifiedon",
          "isdeleted",
          "ownerid",
          "versionstamp",
          "ownername",
          "dwinsertdatetime",
          "dwdbsource",
          "lineitemid",
          "badgename",
          "badgeorg",
          "badgecity",
          "badgestate",
          "attendeddate",
          "canceldate",
          "meta_date_added",
          "meta_date_updated",
          "meta_record_status"
        ]
      },
      {
        "table": "rfrt.dim_event",
        "source": "source.ams_rem_shopping_event",
        "role": "dimension",
        "pk": ["event_key_sk"],
        "businessKeys": ["id", "code"],
        "fks": [],
        "measures": [],
        "attributes": [
          "merchantid",
          "signaturetype",
          "sendconfirmationemail",
          "capacity",
          "numberofguests",
          "allowgroupreg",
          "allowwaitlist",
          "eventstartdate",
          "starttime",
          "eventenddate",
          "type",
          "endtime",
          "earlyregistrationdate",
          "regularregistrationdate",
          "lateregistrationdate",
          "timezonename",
          "isvirtual",
          "createdon",
          "modifiedon",
          "isdeleted",
          "sortorder",
          "versionstamp",
          "name",
          "shortdescription",
          "longdescription",
          "startdate",
          "enddate",
          "ispublic",
          "meta_date_added",
          "meta_date_updated",
          "meta_record_status"
        ]
      },
      {
        "table": "rfrt.dim_session",
        "source": "source.ams_rem_shopping_session",
        "role": "dimension",
        "pk": ["session_key_sk"],
        "businessKeys": ["id", "code"],
        "fks": [
          {
            "from": "eventid",
            "toTable": "rfrt.dim_event",
            "toColumn": "event_key_sk"
          }
        ],
        "measures": [],
        "attributes": [
          "capacity",
          "allowwaitlist",
          "sessionstartdate",
          "starttime",
          "sessionenddate",
          "endtime",
          "earlyregistrationdate",
          "regularregistrationdate",
          "lateregistrationdate",
          "isvirtual",
          "type",
          "eventname",
          "createdon",
          "modifiedon",
          "isdeleted",
          "sortorder",
          "versionstamp",
          "name",
          "shortdescription",
          "longdescription",
          "startdate",
          "enddate",
          "ispublic",
          "meta_date_added",
          "meta_date_updated",
          "meta_record_status"
        ]
      },
      {
        "table": "rfrt.fact_registration_purchases",
        "source": "source.ams_rem_purchase_registrationpurchase",
        "role": "fact",
        "pk": ["registration_purchase_key_sk"],
        "businessKeys": ["id"],
        "fks": [
          {
            "from": "customerid",
            "toTable": "rfrt.dim_customer",
            "toColumn": "customer_key_sk"
          }
        ],
        "measures": [],
        "attributes": [
          "lineitemid",
          "badgename",
          "badgeorg",
          "badgecity",
          "badgestate",
          "attendeddate",
          "canceldate",
          "meta_date_added",
          "meta_date_updated",
          "meta_record_status"
        ]
      },
      {
        "table": "rfrt.fact_payments",
        "source": "source.ams_rem_accounting_payment",
        "role": "fact",
        "pk": ["payment_key_sk"],
        "businessKeys": ["paymentid", "paymentnumber"],
        "fks": [
          {
            "from": "customerid",
            "toTable": "rfrt.dim_customer",
            "toColumn": "customer_key_sk"
          }
        ],
        "measures": [
          "paymentamount",
          "schedulepercent",
          "scheduleamount",
          "totalamountcancelled"
        ],
        "attributes": [
          "createdon",
          "paymentmethod",
          "nameoncheck",
          "checknumber",
          "checkdate",
          "creditcardlast4",
          "referencenumber",
          "authorizationcode",
          "purchaseordernumber",
          "moneyordernumber",
          "wiretransfer",
          "refundauthorizationnumber",
          "routingnumber",
          "vaultnumber",
          "lastcanceldate",
          "lastcancelreferencenumber",
          "merchantname",
          "paymentscheduleid",
          "versionstamp",
          "batchid",
          "paymentschedulenumber",
          "billtocustomeraddressid",
          "name",
          "meta_date_added",
          "meta_date_updated",
          "meta_record_status"
        ]
      }
    ]
  },
  "step3_star_schema": {
    "schema_output": "rfrt",
    "tables": [
      {
        "table": "rfrt.dim_customer",
        "role": "dimension"
      },
      {
        "table": "rfrt.dim_individual",
        "role": "dimension"
      },
      {
        "table": "rfrt.dim_event",
        "role": "dimension"
      },
      {
        "table": "rfrt.dim_session",
        "role": "dimension"
      },
      {
        "table": "rfrt.fact_registrations",
        "role": "fact"
      },
      {
        "table": "rfrt.fact_registration_purchases",
        "role": "fact"
      },
      {
        "table": "rfrt.fact_payments",
        "role": "fact"
      }
    ]
  },
  "step4_relationships": {
    "schema_output": "rfrt",
    "relationships": [
      {
        "fromTable": "rfrt.dim_individual",
        "toTable": "rfrt.dim_customer",
        "fromColumn": "customerid",
        "toColumn": "customer_key_sk",
        "cardinality": "many-to-one",
        "filterDirection": "dim_customer_to_dim_individual"
      },
      {
        "fromTable": "rfrt.fact_registrations",
        "toTable": "rfrt.dim_customer",
        "fromColumn": "customerid",
        "toColumn": "customer_key_sk",
        "cardinality": "many-to-one",
        "filterDirection": "dim_customer_to_fact_registrations"
      },
      {
        "fromTable": "rfrt.fact_registration_purchases",
        "toTable": "rfrt.dim_customer",
        "fromColumn": "customerid",
        "toColumn": "customer_key_sk",
        "cardinality": "many-to-one",
        "filterDirection": "dim_customer_to_fact_registration_purchases"
      },
      {
        "fromTable": "rfrt.fact_payments",
        "toTable": "rfrt.dim_customer",
        "fromColumn": "customerid",
        "toColumn": "customer_key_sk",
        "cardinality": "many-to-one",
        "filterDirection": "dim_customer_to_fact_payments"
      },
      {
        "fromTable": "rfrt.dim_session",
        "toTable": "rfrt.dim_event",
        "fromColumn": "eventid",
        "toColumn": "event_key_sk",
        "cardinality": "many-to-one",
        "filterDirection": "dim_event_to_dim_session"
      }
    ]
  }
}
```

When Step 1 completes, replace the example above with the **actual JSON** returned from the Redshift metadata discovery.

