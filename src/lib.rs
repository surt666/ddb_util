//use rusoto_core::{RusotoError};
use rusoto_dynamodb::{
    AttributeValue, DynamoDb, DynamoDbClient, GetItemInput, QueryInput,
};
use serde::{Deserialize};
use std::collections::HashMap;

pub type DdbMap = HashMap<String, AttributeValue>;

pub fn set_kv(
    item: &mut HashMap<String, AttributeValue>, key: String, val: String,
) -> &HashMap<String, AttributeValue> {
    item.insert(
        key.to_string(),
        AttributeValue {
            s: Some(val.to_string()),
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
pub async fn get_item<'a, T: Deserialize<'a> + Default>(client: &DynamoDbClient, table: &str, key: DdbMap) -> T {
    let get_item_input = GetItemInput {
        key: key,
        table_name: table.to_string(),
        ..Default::default()
    };
    let res = client.get_item(get_item_input)
	.await
	.unwrap()
	.item
	.unwrap();
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
pub async fn query<'a, T: Deserialize<'a>>(client: &DynamoDbClient, table: &str, index_name: Option<String>, key_cond_exp: Option<String>, exp_attr_vals: Option<DdbMap>) -> Vec<T> {    
    let query_input = QueryInput {
        key_condition_expression: key_cond_exp,
        expression_attribute_values: exp_attr_vals,
        table_name: table.to_string(),
        index_name: index_name,
        ..Default::default()
    };
    let items: Vec<T> = client
        .query(query_input)
        .await
        .unwrap()
        .items
        .unwrap_or_else(|| vec![])
        .into_iter()
        .map(|item| serde_dynamodb::from_hashmap(item).unwrap())
        .collect();
        items
}


#[cfg(test)]
mod tests {
    use rusoto_core::{Region};
    use rusoto_dynamodb::{DynamoDbClient};
    use serde::{Deserialize};
    use crate::*;

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
	set_kv(&mut exp_attr, ":itemtype".to_string(), "dataset".to_string());
	let client = DynamoDbClient::new(Region::EuWest1);
	let _x: Vec<Dataset> = query(&client, "relations", Some("".to_string()), Some("itemtype = :itemtype".to_string()), Some(exp_attr)).await;
	Ok(())
    }
    
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

