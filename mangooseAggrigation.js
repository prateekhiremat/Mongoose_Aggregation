const { MongoClient } = require('mongodb');

async function transformData() {
  const uri = 'mongodb://localhost:27017';
  const client = new MongoClient(uri);
  
  try {
    await client.connect();
    const database = client.db('covid_details_db');
    const orders = database.collection('covid_19_india');
    
    const pipeline = [
        {   
          $group:
            {
              _id: "$State/UnionTerritory",
              totalDeaths: {
                $sum: "$Deaths"
              }
            }
        }
      ];

    const results = await orders.aggregate(pipeline).toArray();

    const pipelineOne = [
        {
            $match:
              {
                "State/UnionTerritory": "Karnataka"
              }
        },
        {
          $group:
            {
              _id: "$State/UnionTerritory",
              totalDeaths: {
                $sum: "$Deaths"
              }
            }
        }
      ];

    const resultsOne = await orders.aggregate(pipelineOne).toArray();

    const ordersOne = database.collection('covid_vaccine_statewise');
    const pipelineTwo = [
      {
        $group: {
          _id: "$Updated On",
          totalDosesAdministered: {
            $sum: "$Total Doses Administered"
          }
        }
      }
    ]
    const resultsTwo = await ordersOne.aggregate(pipelineTwo).toArray();

    const pipelineThree = [
      {
        $group: {
          _id: "$Updated On",
          averageFirstDoseAdministered: {
            $avg: "$First Dose Administered"
          }
        }
      }
    ]
    const resultsThree = await ordersOne.aggregate(pipelineThree).toArray();
    
  } finally {
    await client.close();
  }
}

transformData().catch(console.error);