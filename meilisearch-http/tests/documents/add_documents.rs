use crate::common::{GetAllDocumentsOptions, Server};
use actix_web::test;
use chrono::DateTime;
use meilisearch_http::create_app;
use serde_json::{json, Value};

/// This is the basic usage of our API and every other tests uses the content-type application/json
#[actix_rt::test]
async fn add_documents_test_json_content_types() {
    let document = json!([
        {
            "id": 1,
            "content": "Bouvier Bernois",
        }
    ]);

    // this is a what is expected and should work
    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        true,
        &server.service.options
    ))
    .await;
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 202);
    assert_eq!(response, json!({ "updateId": 0 }));
}

/// no content type is still supposed to be accepted as json
#[actix_rt::test]
async fn add_documents_test_no_content_types() {
    let document = json!([
        {
            "id": 1,
            "content": "Montagne des Pyrénées",
        }
    ]);

    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        true,
        &server.service.options
    ))
    .await;
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 202);
    assert_eq!(response, json!({ "updateId": 0 }));
}

/// any other content-type is must be refused
#[actix_rt::test]
async fn add_documents_test_bad_content_types() {
    let document = json!([
        {
            "id": 1,
            "content": "Leonberg",
        }
    ]);

    let server = Server::new().await;
    let app = test::init_service(create_app!(
        &server.service.meilisearch,
        true,
        &server.service.options
    ))
    .await;
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/plain"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(status_code, 415);
    assert_eq!(
        response["message"],
        json!(
            r#"The Content-Type "text/plain" is invalid. Accepted values for the Content-Type header are: "application/json", "application/x-ndjson", "application/csv""#
        )
    );
}

#[actix_rt::test]
async fn add_documents_no_index_creation() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    assert_eq!(response["updateId"], 0);
    /*
     * currently we don’t check these field to stay ISO with meilisearch
     * assert_eq!(response["status"], "pending");
     * assert_eq!(response["meta"]["type"], "DocumentsAddition");
     * assert_eq!(response["meta"]["format"], "Json");
     * assert_eq!(response["meta"]["primaryKey"], Value::Null);
     * assert!(response.get("enqueuedAt").is_some());
     */

    index.wait_update_id(0).await;

    let (response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "processed");
    assert_eq!(response["updateId"], 0);
    assert_eq!(response["type"]["name"], "DocumentsAddition");
    assert_eq!(response["type"]["number"], 1);

    let processed_at =
        DateTime::parse_from_rfc3339(response["processedAt"].as_str().unwrap()).unwrap();
    let enqueued_at =
        DateTime::parse_from_rfc3339(response["enqueuedAt"].as_str().unwrap()).unwrap();
    assert!(processed_at > enqueued_at);

    // index was created, and primary key was infered.
    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], "id");
}

#[actix_rt::test]
async fn document_add_create_index_bad_uid() {
    let server = Server::new().await;
    let index = server.index("883  fj!");
    let (_response, code) = index.add_documents(json!([]), None).await;
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn document_update_create_index_bad_uid() {
    let server = Server::new().await;
    let index = server.index("883  fj!");
    let (response, code) = index.update_documents(json!([]), None).await;
    assert_eq!(code, 400, "{}", response);
}

#[actix_rt::test]
async fn document_addition_with_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "primary": 1,
            "content": "foo",
        }
    ]);
    let (response, code) = index.add_documents(documents, Some("primary")).await;
    assert_eq!(code, 202, "response: {}", response);

    index.wait_update_id(0).await;

    let (response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "processed");
    assert_eq!(response["updateId"], 0);
    assert_eq!(response["type"]["name"], "DocumentsAddition");
    assert_eq!(response["type"]["number"], 1);

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], "primary");
}

#[actix_rt::test]
async fn document_update_with_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "primary": 1,
            "content": "foo",
        }
    ]);
    let (_response, code) = index.update_documents(documents, Some("primary")).await;
    assert_eq!(code, 202);

    index.wait_update_id(0).await;

    let (response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "processed");
    assert_eq!(response["updateId"], 0);
    assert_eq!(response["type"]["name"], "DocumentsPartial");
    assert_eq!(response["type"]["number"], 1);

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], "primary");
}

#[actix_rt::test]
async fn add_documents_with_primary_key_and_primary_key_already_exists() {
    let server = Server::new().await;
    let index = server.index("test");

    index.create(Some("primary")).await;
    let documents = json!([
        {
            "id": 1,
            "content": "foo",
        }
    ]);

    let (_response, code) = index.add_documents(documents, Some("id")).await;
    assert_eq!(code, 202);

    index.wait_update_id(0).await;

    let (response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "failed");

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], "primary");
}

#[actix_rt::test]
async fn update_documents_with_primary_key_and_primary_key_already_exists() {
    let server = Server::new().await;
    let index = server.index("test");

    index.create(Some("primary")).await;
    let documents = json!([
        {
            "id": 1,
            "content": "foo",
        }
    ]);

    let (_response, code) = index.update_documents(documents, Some("id")).await;
    assert_eq!(code, 202);

    index.wait_update_id(0).await;
    let (response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    // Documents without a primary key are not accepted.
    assert_eq!(response["status"], "failed");

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], "primary");
}

#[actix_rt::test]
async fn replace_document() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "doc_id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202, "response: {}", response);

    index.wait_update_id(0).await;

    let documents = json!([
        {
            "doc_id": 1,
            "other": "bar",
        }
    ]);

    let (_response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);

    index.wait_update_id(1).await;

    let (response, code) = index.get_update(1).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "processed");

    let (response, code) = index.get_document(1, None).await;
    assert_eq!(code, 200);
    assert_eq!(response.to_string(), r##"{"doc_id":1,"other":"bar"}"##);
}

// test broken, see issue milli#92
#[actix_rt::test]
#[ignore]
async fn add_no_documents() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.add_documents(json!([]), None).await;
    assert_eq!(code, 200);

    index.wait_update_id(0).await;
    let (response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "processed");
    assert_eq!(response["updateId"], 0);
    assert_eq!(response["success"]["DocumentsAddition"]["nb_documents"], 0);

    let (response, code) = index.get().await;
    assert_eq!(code, 200);
    assert_eq!(response["primaryKey"], Value::Null);
}

#[actix_rt::test]
async fn update_document() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "doc_id": 1,
            "content": "foo",
        }
    ]);

    let (_response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);

    index.wait_update_id(0).await;

    let documents = json!([
        {
            "doc_id": 1,
            "other": "bar",
        }
    ]);

    let (response, code) = index.update_documents(documents, None).await;
    assert_eq!(code, 202, "response: {}", response);

    index.wait_update_id(1).await;

    let (response, code) = index.get_update(1).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "processed");

    let (response, code) = index.get_document(1, None).await;
    assert_eq!(code, 200);
    assert_eq!(
        response.to_string(),
        r##"{"doc_id":1,"content":"foo","other":"bar"}"##
    );
}

#[actix_rt::test]
async fn add_larger_dataset() {
    let server = Server::new().await;
    let index = server.index("test");
    let update_id = index.load_test_set().await;
    let (response, code) = index.get_update(update_id).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "processed");
    assert_eq!(response["type"]["name"], "DocumentsAddition");
    assert_eq!(response["type"]["number"], 77);
    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            limit: Some(1000),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 77);
}

#[actix_rt::test]
async fn update_larger_dataset() {
    let server = Server::new().await;
    let index = server.index("test");
    let documents = serde_json::from_str(include_str!("../assets/test_set.json")).unwrap();
    index.update_documents(documents, None).await;
    index.wait_update_id(0).await;
    let (response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["type"]["name"], "DocumentsPartial");
    assert_eq!(response["type"]["number"], 77);
    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions {
            limit: Some(1000),
            ..Default::default()
        })
        .await;
    assert_eq!(code, 200);
    assert_eq!(response.as_array().unwrap().len(), 77);
}

#[actix_rt::test]
async fn add_documents_bad_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "docid": "foo & bar",
            "content": "foobar"
        }
    ]);
    index.add_documents(documents, None).await;
    index.wait_update_id(0).await;
    let (response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "failed");
}

#[actix_rt::test]
async fn update_documents_bad_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "docid": "foo & bar",
            "content": "foobar"
        }
    ]);
    index.update_documents(documents, None).await;
    index.wait_update_id(0).await;
    let (response, code) = index.get_update(0).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "failed");
}
