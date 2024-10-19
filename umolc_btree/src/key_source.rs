pub struct SourceSlice<'a> {
    prefix: &'a [u8],
    head: u32,
    head_len: u32,
    key: &'a [u8],
}
