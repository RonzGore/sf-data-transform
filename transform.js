const csv = require('csvtojson');
const converter = require('json-2-csv');
const fs = require('fs-extra');

const transformToJSON = async (csvFilePath) => {
    const jsonArray = await csv().fromFile(csvFilePath);
    console.log(jsonArray.length);
    return jsonArray;
}

const tranformToCSV = async (jsonData, destinationPath) => {
    const csv = await converter.json2csvAsync(jsonData, { 'emptyFieldValue': '' });
    fs.writeFileSync('./data/customersMapped.csv', csv);
    console.log('CSV done!')
    
}

// Dedupe the rows based on property
const dedupe = async (data, prop) => {
    const newArray = [];
    var lookupObject  = {};
    for(var i in data) {
        const key = data[i][prop].replace('invalid', ''); 
        lookupObject[key] = data[i];
    }

    for(i in lookupObject) {
        newArray.push(lookupObject[i]);
    }
    return newArray;
}

// find matching dataRecords
const resolveLookup = async (source, destination) => {
    let matchedLeads = [];
    const matchedLeadsSet = new Set()
    const matchedCustomers = destination.map(customerRecord => {
        const matchedLead = source.filter( leadRecord => ( (customerRecord.name.includes(leadRecord.Company) || 
        leadRecord.Company.includes(customerRecord.name)) && 
        (leadRecord.Company !== '.' && leadRecord.Company !== ',') && 
        (customerRecord.name !== '.' && customerRecord.name !== ',') && 
        (leadRecord.Company.length > 2) ) );
        // console.log(matchedLead);
        if(matchedLead.length === 1) {
            //matchedLeads = [...matchedLeads, ...matchedLead];
            matchedLeadsSet.add(matchedLead[0]);
            customerRecord['contact_name'] = matchedLead[0]['Name'];
            customerRecord['contact_email'] = matchedLead[0]['Email'];
        } else {
            customerRecord['contact_name'] = '';
            customerRecord['contact_email'] = '';
        }
        
        return customerRecord;
    });
    matchedLeads = Array.from(matchedLeadsSet);
    console.log(matchedLeads.length);
    console.log(matchedLeadsSet.size);
    console.log(matchedLeads[0]);
    console.log(source[0]);
    const unmatchedLeads = source.filter( ar => !matchedLeads.find(rm => rm.Company === ar.Company  ) );
    console.log(unmatchedLeads.length);
    const unmatchedCustomers = unmatchedLeads.map(unmatchedCustomer => {
        return {
            'name': unmatchedCustomer.Company,
            'contact_name': unmatchedCustomer.Name,
            'contact_email': unmatchedCustomer.Email
        }
    })
    return [...matchedCustomers, ...unmatchedCustomers];
}

const transform = async () => {
    const sourceJSONData = await transformToJSON('./data/leads.csv');
    console.log(sourceJSONData[0]);
    const dedupedJSONData = await dedupe(sourceJSONData, 'Email');
    console.log(dedupedJSONData.length);
    console.log(dedupedJSONData[0]);
    console.log(dedupedJSONData[dedupedJSONData.length-1]);
    const destinationData = await transformToJSON('./data/customers.csv');
    console.log(destinationData[0]);
    console.log(destinationData[destinationData.length-1]);
    const finalData = await resolveLookup(dedupedJSONData, destinationData);
    console.log(finalData.length);
    console.log(finalData[0]);
    console.log(finalData[finalData.length-1]);
    tranformToCSV(finalData, '');
}

transform();
