// TODO:
struct tree_node<'a> {
    page: &'a mut [u8; 4096],
}

impl<'a> tree_node<'a> {
    //
    pub fn new(page: &'a mut [u8; 4096]) -> Self {
        Self { page: page }
    }
}
