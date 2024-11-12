#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Supreme<T> {
    X(T),
    Sup,
}
