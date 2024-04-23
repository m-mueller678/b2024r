use itertools::Itertools;
use proc_macro::TokenStream as TokenStream1;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Index, Member, Meta, Path, Token, Type, Visibility};

fn seqlock_crate() -> TokenStream {
    quote! {seqlock}
}

fn path_is_ident(p: &Path, ident: &str) -> bool {
    p.get_ident().map(|x| x.to_string()).as_deref() == Some(ident)
}

fn extract_wrapper_attr(x: &[Attribute]) -> impl Iterator<Item = Path> + '_ {
    x.iter().filter_map(|x| match &x.meta {
        Meta::List(x) if path_is_ident(&x.path, "seq_lock_wrapper") => {
            let tokens: TokenStream1 = (x.tokens.clone()).into();
            let path = syn::parse::<Path>(tokens).expect("cannot parse seq_lock_wrapper path");
            // let path:Path=parse_macro_input!(tokens as Path);
            Some(path)
        }
        _ => None,
    })
}

struct AccessorSpec {
    vis: Visibility,
    name: Ident,
    _colon_token: Token![:],
    ty: Type,
    _eq_token: Token![=],
    expr: Punctuated<Member, Token![.]>,
}

impl Parse for AccessorSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(AccessorSpec {
            vis: input.parse()?,
            name: input.parse()?,
            _colon_token: input.parse()?,
            ty: input.parse()?,
            _eq_token: input.parse()?,
            expr: Punctuated::parse_separated_nonempty(input)?,
        })
    }
}

#[proc_macro_derive(SeqlockAccessors, attributes(seq_lock_wrapper, seq_lock_skip_accessor, seq_lock_accessor))]
pub fn derive_seqlock_safe(input: TokenStream1) -> TokenStream1 {
    let input = parse_macro_input!(input as DeriveInput);
    let wrapper_path =
        extract_wrapper_attr(&input.attrs).exactly_one().map_err(|_| ()).expect("need exactly one seq_lock_wrapper");
    let seqlock = seqlock_crate();
    let accessors = match &input.data {
        Data::Struct(s) => {
            let default_accessors = s
                .fields
                .iter()
                .enumerate()
                .filter(|(_i, field)| {
                    !field.attrs.iter().any(|attr| match &attr.meta {
                        Meta::Path(x) => path_is_ident(x, "seq_lock_skip_accessor"),
                        _ => false,
                    })
                })
                .map(|(i, field)| {
                    let name = field.ident.clone().unwrap_or_else(|| format_ident!("x{i}"));
                    let member = field.ident.clone().map(Member::Named).unwrap_or(Member::Unnamed(Index::from(i)));
                    AccessorSpec {
                        vis: field.vis.clone(),
                        name,
                        _colon_token: field.colon_token.unwrap_or_default(),
                        ty: field.ty.clone(),
                        _eq_token: Default::default(),
                        expr: std::iter::once(member).collect(),
                    }
                });
            let custom_accessors = input.attrs.iter().filter_map(|x| match &x.meta {
                Meta::List(x) if path_is_ident(&x.path, "seq_lock_accessor") => {
                    Some(syn::parse::<AccessorSpec>(x.tokens.clone().into()).unwrap())
                }
                _ => None,
            });
            default_accessors.chain(custom_accessors)
                .flat_map(|AccessorSpec {  name, ty, expr, vis,.. }| {
                    let versions = [
                        (format_ident!("{name}_mut"), quote!(mut), quote!(SeqLockModeParam)),
                        (name,quote!(),quote!(SeqLockModeParam::SharedDowngrade)),
                    ];
                    versions.map(|(name,mutable,return_mode)|quote!(
                        #vis fn #name<'b>(&'b #mutable self)-><#ty as #seqlock::SeqLockWrappable>::Wrapper<#seqlock::Guarded<'b,#return_mode,#ty>> {
                            unsafe{
                                #seqlock::Guarded::wrap_unchecked(
                                    core::ptr::addr_of_mut!((*self.0.as_ptr()).#expr)
                                )
                            }
                        }
                    ))

                })
        }
        Data::Enum(_) => panic!(),
        Data::Union(_) => panic!(),
    };
    let name = input.ident;
    let generic_params = input.generics.params.clone();
    let (impl_generics, impl_type_generics, impl_generics_where) = input.generics.split_for_impl();

    let out = quote! {
        impl #impl_generics #seqlock::SeqLockWrappable for #name #impl_type_generics #impl_generics_where{
            type Wrapper<WrappedParam> = #wrapper_path<WrappedParam>;
        }

        impl<'wrapped_guard,SeqLockModeParam:#seqlock::SeqLockMode,#generic_params>
        #wrapper_path<
            #seqlock::Guarded<'wrapped_guard,SeqLockModeParam,#name #impl_type_generics>
        >
        #impl_generics_where{
                    #(#accessors)*
        }
    };

    TokenStream1::from(out)
}
