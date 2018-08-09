 

This README would normally document whatever steps are necessary to get your application up and running.

What is this repository for? 

This is Python Script for extracting Google analytics data for the any Client into Bigquery .This uses API v4.

Please see ga_config.json for credentials which are required for this script.

GA has a limitation of pulling only 10 metrics and 7 dimensions. As we had requirement to pull more than 10 metrics we are looping thru and have created 10 Goal staging tables .So that we can pull all 20 goals.

First we are pulling 10 metrics in HBF_GA_Metrics_STaging table in Bigquery
then pulling all 20 goals in 10 goal staging tables (HBF_GA_GoalSet*) where * is from 1 to 10
Then all these tables are merged and joined in one main staging table HBF_GA_Staging.
Once all the required data comes to HBF_GA_Staging ,we append the same to HBF_GA_Master Table.


Variables to be passed from Jenkins:

startDATE (Optional) (From when to fetch the data) (If you dont supply it calculates as yesterday or based on past runs) 
endDATE (Optional) (Till when to fetch the data) (If you dont supply it calculates as yesterday) 
viewID : (Optional). If not passed then it will fetch data for the folloing VIEW_ID_LIST = ['114994744', '25387584', '92766716', '93635313', '91539947', '93070953', '175878046']



Who do I talk to? Contact Harmeet  