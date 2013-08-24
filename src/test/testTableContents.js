var azure = require('azure');

var tableService = azure.createTableService();
var query = azure.TableQuery
    .select()
    .from('test')
//    .where('PartitionKey eq ?', 'A');

tableService.queryEntities(query, function(error, entities){
    if(!error){
        //entities contains an array of entities
    }
    console.log(entities);
});