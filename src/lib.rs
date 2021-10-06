//use rusoto_core::{RusotoError};
use itertools::Itertools;
use rusoto_dynamodb::{
    AttributeValue, BatchWriteItemInput, DeleteRequest, DynamoDb, DynamoDbClient, GetItemInput,
    PutItemInput, PutItemOutput, PutRequest, QueryInput, WriteRequest,
};
use serde::Deserialize;
use std::collections::HashMap;

pub type DdbMap = HashMap<String, AttributeValue>;

pub fn set_kv(
    item: &mut HashMap<String, AttributeValue>, key: String, val: String,
) -> &HashMap<String, AttributeValue> {
    item.insert(
        key,
        AttributeValue {
            s: Some(val),
            ..Default::default()
        },
    );
    item
}

/// # Dynamodb query function
/// ```
/// # use rusoto_core::{Region, RusotoError};
/// # use rusoto_dynamodb::{
/// #     AttributeValue, DynamoDb, DynamoDbClient, QueryError, QueryInput,
/// # };
/// # use serde::{Deserialize};
/// # use std::collections::HashMap;
/// # use ddb_util::*;
///
/// # #[derive(Debug)]
/// # struct Dataset {
/// #     pk: String,
/// #     sk: String,
/// #     itemtype: String,
/// #     created: Option<u64>,
/// # }
///
/// # #[tokio::test]
/// # async fn try_ddb_util_main() -> Result<(), String> {
/// let client = DynamoDbClient::new(Region::EuWest1);
/// let mut key: DdbMap = HashMap::new();
/// set_kv(&mut key, "pk".to_string(), "c4c".to_string());
/// set_kv(&mut key, "sk".to_string(), "c4c".to_string());
/// let x: Dataset = get_item(&client, "relations", key).await;
/// #     Ok(())
/// # }
/// ```
pub async fn get_item<'a, T: Deserialize<'a> + Default>(
    client: &DynamoDbClient, table: &str, key: DdbMap,
) -> T {
    let get_item_input = GetItemInput {
        key,
        table_name: table.to_string(),
        ..Default::default()
    };
    let res = client.get_item(get_item_input).await.unwrap().item.unwrap();
    serde_dynamodb::from_hashmap(res).unwrap()
}

/// # Dynamodb query function
/// ```
/// use rusoto_core::{Region, RusotoError};
/// use rusoto_dynamodb::{
///     AttributeValue, DynamoDb, DynamoDbClient, QueryError, QueryInput,
/// };
/// use serde::{Deserialize};
/// use std::collections::HashMap;
/// use ddb_util::*;
///
/// # #[derive(Debug)]
/// # struct Dataset {
/// #     pk: String,
/// #     sk: String,
/// #     itemtype: String,
/// #     created: Option<u64>,
/// # }
///
/// # #[tokio::test]
/// # async fn try_ddb_util_main() -> Result<(), String> {
/// let client = DynamoDbClient::new(Region::EuWest1);
/// let x: Vec<Dataset> = query(&client, "relations", "dataset").await;
/// #     Ok(())
/// # }
/// ```
pub async fn query<'a, T: Deserialize<'a>>(
    client: &DynamoDbClient, table: &str, index_name: Option<String>, key_cond_exp: Option<String>,
    exp_attr_vals: Option<DdbMap>, exp_attr_names: Option<HashMap<String, String>>, projection_exp: Option<String>,
    filter_exp: Option<String>) -> Vec<T> {
    let query_input = QueryInput {
        key_condition_expression: key_cond_exp,
        expression_attribute_values: exp_attr_vals,
        expression_attribute_names: exp_attr_names,
        projection_expression: projection_exp,
        filter_expression: filter_exp,
        table_name: table.to_string(),
        index_name,
        ..Default::default()
    };
    let items: Vec<T> = client
        .query(query_input)
        .await
        .unwrap()
        .items
        .unwrap_or_else(|| vec![])
        .into_iter()
        .map(|item| {
            serde_dynamodb::from_hashmap(item).unwrap()
        })
        .collect();
    items
}

pub async fn put_item(client: &DynamoDbClient, table: &str, item: DdbMap) -> PutItemOutput {
    let input = PutItemInput {
        table_name: table.to_string(),
        item,
        ..Default::default()
    };
    let res = client.put_item(input).await.unwrap();
    res
}

fn create_write_request(
    write_items: Option<Vec<DdbMap>>, delete_items: Option<Vec<DdbMap>>,
) -> Vec<WriteRequest> {
    let mut dwr: Vec<WriteRequest>;
    if let Some(di) = delete_items {
        dwr = di
            .into_iter()
            .map(|x| WriteRequest {
                delete_request: Some(DeleteRequest { key: x }),
                put_request: None,
            })
            .collect();
    } else {
        dwr = vec![]
    }
    if let Some(wi) = write_items {
        let pwr: Vec<WriteRequest> = wi
            .into_iter()
            .map(|x| WriteRequest {
                delete_request: None,
                put_request: Some(PutRequest { item: x }),
            })
            .collect();
        dwr.extend(pwr);
    }
    dwr
}

pub async fn batch_write_items(
    client: &DynamoDbClient, table: &str, write_items: Option<Vec<DdbMap>>,
    delete_items: Option<Vec<DdbMap>>,
) -> Vec<WriteRequest> {
    let mut vector: Vec<WriteRequest> = Vec::new();
    let v = create_write_request(write_items, delete_items);
    for chunk in &v.into_iter().chunks(25) {
        let c: Vec<WriteRequest> = chunk.collect();
        let mut m = HashMap::new();
        m.insert(table.to_string(), c);
        let input = BatchWriteItemInput {
            request_items: m,
            ..Default::default()
        };
        let res = client.batch_write_item(input).await.unwrap();
        if let Some(m) = res.unprocessed_items {
            if let Some(e) = m.get(table) {
                vector.extend(e.clone())
            }
        }
    }
    vector
}

#[cfg(test)]
mod tests {
    use crate::*;
    use rusoto_core::Region;
    use rusoto_dynamodb::DynamoDbClient;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct Dataset {
        pk: String,
        sk: String,
        itemtype: String,
        created: Option<u64>,
    }

    #[tokio::test]
    async fn try_ddb_util_main() -> Result<(), String> {
        let mut exp_attr: DdbMap = HashMap::new();
        set_kv(
            &mut exp_attr,
            ":itemtype".to_string(),
            "dataset".to_string(),
        );
        let client = DynamoDbClient::new(Region::EuWest1);
        let _x: Vec<Dataset> = query(
            &client,
            "relations",
            Some("".to_string()),
            Some("itemtype = :itemtype".to_string()),
            Some(exp_attr),
            None,
            None,
            None
        )
        .await;
        Ok(())
    }
}
