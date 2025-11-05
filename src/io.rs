struct AlignedBuffer {
    ptr: *mut u8,
    size: usize,
    alignment: usize,
}

impl AlignedBuffer {
    // pub fn new(size: usize) -> AlignedBuffer {
    //     AlignedBuffer{
    //         ptr: (),
    //         size,
    //         alignment: 0,
    //     }
    // }
}