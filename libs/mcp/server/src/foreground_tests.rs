use super::tests::local_container_with_profile;
use super::*;
use rmcp::service::PeerRequestOptions;
use rmcp::{ServiceExt, model::ClientRequest};

async fn wait_for_marker(path: &Path) -> u32 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(text) = fs::read_to_string(path)
                && let Ok(pid) = text.trim().parse()
            {
                return pid;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("nested shell must author its start marker")
}

async fn assert_gone(pid: u32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let stat = fs::read_to_string(format!("/proc/{pid}/stat"));
            // A killed orphan may briefly remain a zombie under CI's
            // init; it cannot execute or create the delayed artifact.
            if stat.as_ref().map_or(true, |s| {
                s.rsplit_once(") ")
                    .is_some_and(|(_, rest)| rest.starts_with('Z'))
            }) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owned shell must stop executing");
}

async fn exercise(kind: &str, configured_timeout: Option<u64>) {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("started");
    let late = dir.path().join("late");
    let outer = dir.path().join("outer");
    let sleeper = dir.path().join("sleeper");
    let command = format!(
        r#"echo $$ > "{}"; sh -c 'echo $$ > "{}"; sleep 2 & echo $! > "{}"; wait; echo late > "{}"' & wait"#,
        outer.display(), marker.display(), sleeper.display(), late.display()
    );
    let mut control = Command::new("sleep").arg("30").kill_on_drop(true).spawn().unwrap();
    let (client_io, server_io) = tokio::io::duplex(65536);
    let server = tokio::spawn(async move {
        local_container_with_profile(None)
            .serve(server_io)
            .await
            .unwrap()
    });
    let client = ().serve(client_io).await.unwrap();
    let server = server.await.unwrap();
    let request = client
        .send_cancellable_request(
            ClientRequest::CallToolRequest(Request::new(CallToolRequestParam {
                name: "run_command".into(),
                arguments: Some(
                    serde_json::json!({
                        "command": command,
                        "timeout": configured_timeout,
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                ),
                ..Default::default()
            })),
            PeerRequestOptions::default(),
        )
        .await
        .unwrap();
    let pid = wait_for_marker(&marker).await;
    let outer_pid = wait_for_marker(&outer).await;
    let sleeper_pid = wait_for_marker(&sleeper).await;
    if kind == "disconnect" {
        client.cancel().await.unwrap();
        server.cancel().await.unwrap();
    } else {
        if kind == "cancel" {
            client
                .notify_cancelled(CancelledNotificationParam {
                    request_id: request.id.clone(),
                    reason: Some("regression cancellation".into()),
                })
                .await
                .unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(5), request.await_response())
            .await
            .expect("executor must settle boundedly");
        if kind == "timeout" {
            let result = result.unwrap();
            assert!(format!("{result:?}").contains("timed out"));
        }
        // The same MCP session must still execute a subsequent command.
        let next = client
            .call_tool(CallToolRequestParam {
                name: "run_command".into(),
                arguments: Some(
                    serde_json::json!({"command": "printf subsequent"})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(format!("{next:?}").contains("subsequent"));
        client.cancel().await.unwrap();
        server.cancel().await.unwrap();
    }
    assert_gone(pid).await;
    assert_gone(outer_pid).await;
    assert_gone(sleeper_pid).await;
    assert!(
        control.try_wait().unwrap().is_none(),
        "unrelated process survives"
    );
    sleep(Duration::from_millis(2200)).await;
    assert!(!late.exists(), "cancelled descendant must not write later");
    assert!(control.try_wait().unwrap().is_none(), "control survives deadline");
    control.kill().await.unwrap();
}

#[tokio::test]
async fn foreground_cancel_without_timeout() {
    exercise("cancel", None).await;
}

#[tokio::test]
async fn foreground_cancel_with_timeout() {
    exercise("cancel", Some(30)).await;
}

#[tokio::test]
async fn foreground_timeout() {
    exercise("timeout", Some(1)).await;
}

#[tokio::test]
async fn foreground_disconnect() {
    exercise("disconnect", None).await;
}
