select "Date" as date , month(cast("Date" as date))  as month from {{source('raw','ADVENTUREWORKS_CALENDAR')}}
