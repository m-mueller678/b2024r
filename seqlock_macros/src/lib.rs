use itertools::Itertools;
use proc_macro::TokenStream as TokenStream1;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Attribute, Data, DeriveInput, Meta, Path};

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
            let path = syn::parse::<Path>(tokens).unwrap();
            // let path:Path=parse_macro_input!(tokens as Path);
            Some(path)
        }
        _ => None,
    })
}

#[proc_macro_derive(SeqlockAccessors, attributes(seq_lock_wrapper, seq_lock_skip_accessor))]
pub fn derive_seqlock_safe(input: TokenStream1) -> TokenStream1 {
    let input = parse_macro_input!(input as DeriveInput);
    let wrapper_path = extract_wrapper_attr(&input.attrs)
        .exactly_one()
        .map_err(|_| ())
        .expect("need exactly one seq_lock_wrapper");
    let seqlock = seqlock_crate();
    let accessors = match &input.data {
        Data::Struct(s) => {
            s.fields.iter()
                .filter(|field|{
                    !field.attrs.iter().any(|attr|{
                        match &attr.meta {
                            Meta::Path(x) => path_is_ident(x,"seq_lock_skip_accessor"),
                            _=>false
                        }
                    })
                })
                .map(|field| {
                let name = field.ident.as_ref().unwrap();
                let ty = &field.ty;
                let vis = &field.vis;
                quote!(
                    #vis fn #name<'b>(&'b mut self)-><#ty as #seqlock::SeqLockWrappable>::Wrapper<#seqlock::Guarded<'b,SeqLockModeParam,#ty>>{
                        unsafe{
                            #seqlock::Guarded::<SeqLockModeParam,#ty>::wrap_unchecked(
                                core::ptr::addr_of_mut!((*self.0.as_ptr()).#name)
                            )
                        }
                    }
                )
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

            fn wrap<WrappedParam>(x: WrappedParam) -> Self::Wrapper<WrappedParam> {
                #wrapper_path(x)
            }

            fn get<WrappedParam>(x: &Self::Wrapper<WrappedParam>) -> &WrappedParam {
                &x.0
            }

            fn get_mut<WrappedParam>(x: &mut Self::Wrapper<WrappedParam>) -> &mut WrappedParam {
                &mut x.0
            }
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
