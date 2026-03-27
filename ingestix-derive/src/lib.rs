// Copyright (c) 2026 Magnus Karlsson and Ingestix contributors
// Licensed under the Apache License, Version 2.0 or the MIT license.

//! # Ingestix Derive
//!
//! Procedural macro support for `ingestix`.
//!
//! # High-level Overview
//!
//! This crate provides the `#[derive(FlowWorker)]` macro, which generates a
//! `ingestix::Worker` implementation for user-defined worker structs.
//!
//! # Architecture
//!
//! The macro parses `#[flow(...)]` attributes at compile time and emits an async
//! `Worker::process` method that forwards to a user-defined `handle` method.
//! This keeps runtime overhead minimal while reducing repetitive trait boilerplate.
use proc_macro::TokenStream;
use quote::quote;
use syn::{
    DeriveInput, LitStr, Meta, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

struct FlowArgs {
    message_type: syn::Type,
    config_type: syn::Type,
    state_type: syn::Type,
}

impl Parse for FlowArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut message_type = None;
        let mut config_type = None;
        let mut state_type = None;

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let lit: LitStr = input.parse()?;
            let ty: syn::Type = syn::parse_str(&lit.value())?;
            match ident.to_string().as_str() {
                "message" => message_type = Some(ty),
                "config" => config_type = Some(ty),
                "state" => state_type = Some(ty),
                _ => {
                    return Err(syn::Error::new_spanned(
                        ident,
                        "Expected message, config, or state",
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(FlowArgs {
            message_type: message_type
                .ok_or_else(|| syn::Error::new(input.span(), "Missing message"))?,
            config_type: config_type
                .ok_or_else(|| syn::Error::new(input.span(), "Missing config"))?,
            state_type: state_type.ok_or_else(|| syn::Error::new(input.span(), "Missing state"))?,
        })
    }
}

/// Generate an `ingestix::Worker` implementation for your worker struct.
///
/// The macro expects you to provide a `#[flow(...)]` attribute specifying your types:
/// - `message`: the message type received from the core queue
/// - `config`: the ingest configuration type stored in `SharedContext`
/// - `state`: the shared application state type stored in `SharedContext`
///
/// It then generates `Worker::process(...)` that forwards to a method named `handle` on
/// your struct.
///
/// # Example
/// ```text
/// use ingestix::SharedContext;
///
/// #[derive(FlowWorker)]
/// #[flow(message="MyMsg", config="MyConfig", state="MyState")]
/// struct MyWorker;
///
/// impl MyWorker {
///     async fn handle(
///         &self,
///         msg: MyMsg,
///         ctx: std::sync::Arc<SharedContext<MyConfig, MyState>>,
///     ) -> anyhow::Result<()> {
///         let _ = (msg, ctx);
///         Ok(())
///     }
/// }
/// ```
#[proc_macro_derive(FlowWorker, attributes(flow))]
pub fn derive_flow_worker(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let mut args: Option<FlowArgs> = None;
    let mut parse_error: Option<syn::Error> = None;

    for attr in input.attrs {
        if !attr.path().is_ident("flow") {
            continue;
        }
        if let Meta::List(list) = attr.meta {
            match list.parse_args::<FlowArgs>() {
                Ok(v) => args = Some(v),
                Err(e) => parse_error = Some(e),
            }
        }
    }

    if let Some(err) = parse_error {
        return err.to_compile_error().into();
    }

    let args = match args {
        Some(v) => v,
        None => {
            return syn::Error::new(
                name.span(),
                "derive(FlowWorker) requires #[flow(message=\"..\", config=\"..\", state=\"..\")]",
            )
            .to_compile_error()
            .into();
        }
    };
    let (msg, cfg, state) = (args.message_type, args.config_type, args.state_type);

    let expanded = quote! {
        #[ingestix::async_trait]
        impl ingestix::Worker<#msg, #cfg, #state> for #name {
            async fn process(
                &self,
                msg: #msg,
                ctx: std::sync::Arc<ingestix::SharedContext<#cfg, #state>>
            ) -> anyhow::Result<()> {
                self.handle(msg, ctx).await
            }
        }
    };
    TokenStream::from(expanded)
}
