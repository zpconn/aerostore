use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, spanned::Spanned, Fields, ItemStruct};

#[proc_macro_attribute]
pub fn speedtable(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as ItemStruct);
    let struct_ident = input.ident.clone();

    let named_fields = match &mut input.fields {
        Fields::Named(fields) => fields,
        _ => {
            return syn::Error::new(
                input.span(),
                "#[speedtable] supports only structs with named fields",
            )
            .to_compile_error()
            .into();
        }
    };

    if named_fields.named.len() > 64 {
        return syn::Error::new(
            input.span(),
            "#[speedtable] supports at most 64 user fields (u64 null bitmask)",
        )
        .to_compile_error()
        .into();
    }

    for field in &named_fields.named {
        if let Some(ident) = &field.ident {
            let name = ident.to_string();
            if matches!(
                name.as_str(),
                "_null_bitmask" | "_nullmask" | "_xmin" | "_xmax"
            ) {
                return syn::Error::new(
                    ident.span(),
                    "field name is reserved by #[speedtable]: _null_bitmask, _xmin, _xmax",
                )
                .to_compile_error()
                .into();
            }
        }
    }

    let user_fields: Vec<_> = named_fields
        .named
        .iter()
        .filter_map(|f| f.ident.clone().map(|ident| (ident, f.ty.clone())))
        .collect();

    let arg_idents: Vec<_> = user_fields.iter().map(|(ident, _)| ident).collect();
    let arg_types: Vec<_> = user_fields.iter().map(|(_, ty)| ty).collect();

    let null_bit_consts: Vec<_> = user_fields
        .iter()
        .enumerate()
        .map(|(idx, (ident, _))| {
            let const_ident = format_ident!("__NULLBIT_{}", to_screaming_snake(&ident.to_string()));
            let idx_lit = idx as u8;
            quote! {
                #[doc(hidden)]
                pub const #const_ident: u8 = #idx_lit;
            }
        })
        .collect();

    let null_index_match_arms: Vec<_> = user_fields
        .iter()
        .map(|(ident, _)| {
            let field_name = ident.to_string();
            let const_ident = format_ident!("__NULLBIT_{}", to_screaming_snake(&field_name));
            quote! {
                #field_name => Some(Self::#const_ident),
            }
        })
        .collect();

    named_fields.named.push(syn::parse_quote! {
        #[doc(hidden)]
        pub(crate) _null_bitmask: u64
    });
    named_fields.named.push(syn::parse_quote! {
        #[doc(hidden)]
        pub(crate) _xmin: u64
    });
    named_fields.named.push(syn::parse_quote! {
        #[doc(hidden)]
        pub(crate) _xmax: ::std::sync::atomic::AtomicU64
    });

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        #input

        impl #impl_generics #struct_ident #ty_generics #where_clause {
            #[inline]
            pub fn new(#(#arg_idents: #arg_types),*) -> Self {
                Self {
                    #(#arg_idents,)*
                    _null_bitmask: 0,
                    _xmin: 0,
                    _xmax: ::std::sync::atomic::AtomicU64::new(0),
                }
            }

            #(#null_bit_consts)*

            #[inline]
            pub fn __set_null_by_index(&mut self, idx: u8, is_null: bool) {
                let bit = 1_u64 << idx;
                if is_null {
                    self._null_bitmask |= bit;
                } else {
                    self._null_bitmask &= !bit;
                }
            }

            #[inline]
            pub fn __is_null_by_index(&self, idx: u8) -> bool {
                (self._null_bitmask & (1_u64 << idx)) != 0
            }

            #[inline]
            pub fn __null_bitmask(&self) -> u64 {
                self._null_bitmask
            }

            #[inline]
            pub fn __null_index_for_field(field: &str) -> Option<u8> {
                match field {
                    #(#null_index_match_arms)*
                    _ => None,
                }
            }

            #[inline]
            pub fn __is_null_field(&self, field: &str) -> bool {
                Self::__null_index_for_field(field)
                    .map(|idx| self.__is_null_by_index(idx))
                    .unwrap_or(false)
            }

            #[inline]
            pub fn __xmin(&self) -> u64 {
                self._xmin
            }

            #[inline]
            pub fn __xmax(&self) -> u64 {
                self._xmax.load(::std::sync::atomic::Ordering::Acquire)
            }
        }
    };

    expanded.into()
}

fn to_screaming_snake(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 2);
    let mut prev_is_lower = false;

    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() && prev_is_lower {
                out.push('_');
            }
            out.push(ch.to_ascii_uppercase());
            prev_is_lower = ch.is_ascii_lowercase();
        } else {
            if !out.ends_with('_') {
                out.push('_');
            }
            prev_is_lower = false;
        }
    }

    out.trim_matches('_').to_string()
}
