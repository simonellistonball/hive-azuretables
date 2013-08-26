#-------------------------------------------------------------------------------
# Copyright 2013 Simon Elliston Ball
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#-------------------------------------------------------------------------------
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