use seqlock::{Guarded, SeqLockMode};

pub fn common_prefix<M: SeqLockMode>(a: &[Guarded<'_, M, [u8]>], b: &[Guarded<'_, M, [u8]>]) -> usize
where
    for<'a> Guarded<'a, M, [u8]>: Copy,
{
    let a_bytes = a.iter().copied().flat_map(|x| x.iter());
    let b_bytes = b.iter().copied().flat_map(|x| x.iter());
    a_bytes.zip(b_bytes).take_while(|(a, b)| a.load() == b.load()).count()
}
