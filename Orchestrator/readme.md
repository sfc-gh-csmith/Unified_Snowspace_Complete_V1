This is the readme file for the Orchestrator Unified Snowspace App

For each Database you wish to use with the app, you need to make the following grants:

grant usage on database <DB_NAME> to application <NAME_OF_THIS_APP>;
grant usage on all schemas in DATABASE <DB_NAME> to application <NAME_OF_THIS_APP>;
grant select on all tables in database <DB_NAME> to application <NAME_OF_THIS_APP>;

For the time being it is also required to manually share, since cross cloud auto fufillment does not work like direct shares in the same cloud/region.  

