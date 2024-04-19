pub fn common_prefix(a:&[u8],b:&[u8])->usize{
    let mut i=0;
    loop{
        if i<a.len() && i<b.len() && a[i]==b[i]{
            i+=1;
        }else{
            break i
        }
    }
}