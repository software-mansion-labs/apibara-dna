use std::pin::Pin;
use std::task::{Context, Poll};

use error_stack::{Result, ResultExt};
use futures::stream::SplitStream;
use futures::{SinkExt, Stream, StreamExt};
use starknet_rust::core::types::ConfirmedBlockId;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::http::StarknetProviderError;

/// The websocket stream is used only to drive the polling loop.
/// There is no need to parse the messages, as they are discarded.
#[derive(Debug)]
pub struct NewHeadMessage;

/// A stream of new heads from a Starknet websocket subscription.
pub struct NewHeadsStream {
    inner: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

#[derive(Debug)]
struct SubscribeRequest {
    block_id: ConfirmedBlockId,
}

impl NewHeadsStream {
    /// Creates a new [`NewHeadsStream`] from a websocket URL.
    pub async fn connect(url: &str) -> Result<Self, StarknetProviderError> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(url)
            .await
            .change_context(StarknetProviderError::Request)
            .attach_printable("failed to connect to ws stream")?;

        let (mut write, read) = ws_stream.split();

        // Since we are only subscribing to new heads, we don't need to keep the
        // tx around to then send messages.
        // Just subscribe and then give up the read half.
        write
            .send(
                SubscribeRequest {
                    block_id: ConfirmedBlockId::Latest,
                }
                .into(),
            )
            .await
            .change_context(StarknetProviderError::Request)
            .attach_printable("failed to send subscribe request")?;

        Ok(Self { inner: read })
    }
}

impl NewHeadMessage {
    pub fn try_from_message(msg: Message) -> Result<Option<Self>, StarknetProviderError> {
        let Message::Text(_text) = msg else {
            return Ok(None);
        };

        Ok(NewHeadMessage.into())
    }
}

impl Stream for NewHeadsStream {
    type Item = Result<NewHeadMessage, StarknetProviderError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(msg))) => match NewHeadMessage::try_from_message(msg) {
                Ok(None) => Poll::Pending,
                Ok(Some(msg)) => Poll::Ready(Some(Ok(msg))),
                Err(e) => Poll::Ready(Some(Err(e))),
            },
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(e).change_context(StarknetProviderError::Request)))
            }
        }
    }
}

impl SubscribeRequest {
    pub fn into_string(self) -> String {
        use serde_json::json;
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "starknet_subscribeNewHeads",
            "params": [self.block_id]
        });
        serde_json::to_string(&payload).expect("serialization")
    }
}

impl From<SubscribeRequest> for Message {
    fn from(r: SubscribeRequest) -> Self {
        Message::Text(r.into_string().into())
    }
}
