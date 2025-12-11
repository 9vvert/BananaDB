use BananaDB::io_manager::cache_system;
use BananaDB::io_manager::cache_system::resource::PageType;
use BananaDB::io_manager::cache_system::resource::ResId;

#[test]
fn test_cache() {
    let mut cache_sys = cache_system::CacheBuf::<3>::new();
    let page1 = ResId::new(PageType::TABLE, "file1", 1);
    let page2 = ResId::new(PageType::TABLE, "file1", 2);
    let page3 = ResId::new(PageType::TABLE, "file1", 3);
    let page4 = ResId::new(PageType::TABLE, "file2", 4);
    let page5 = ResId::new(PageType::TABLE, "file1", 5);

    assert!(cache_sys.query_cache_index(&page1).is_none());
    assert!(cache_sys.query_cache_index(&page2).is_none());

    let mut buffer = [0u8; 4096];
    let mut buffer1 = [0u8; 4096];
    let mut buffer2 = [0u8; 4096];
    let mut buffer3 = [0u8; 4096];
    let mut buffer4 = [0u8; 4096];

    for i in 0..buffer.len() {
        buffer[i] = (i % 256) as u8;
        buffer1[i] = (i % 256) as u8;
        buffer2[i] = (i % 256) as u8;
        buffer3[i] = (i % 256) as u8;
        buffer4[i] = (i % 256) as u8;
    }

    // 填充缓存页
    cache_sys.add_cache_resource(&page3, buffer);
    assert!(cache_sys.query_cache_index(&page1).is_none());
    assert!(cache_sys.query_cache_index(&page2).is_none());

    let opt_cid3 = cache_sys.query_cache_index(&page3);
    assert!(opt_cid3.is_some());
    let cid3 = opt_cid3.unwrap();

    let mut tmp_cid;

    // 获得缓存
    let buffer = cache_sys.get_cache_resource(cid3);

    for i in 0..15 {
        println!("{}", buffer[i]);
    }
    println!("0--------------------------------------");
    cache_sys.debug_cache();

    // 访问一个新的页page1
    println!("1--------------------------------------");
    cache_sys.add_cache_resource(&page1, buffer1);
    tmp_cid = cache_sys.query_cache_index(&page1).unwrap();
    cache_sys.get_cache_resource(tmp_cid);
    cache_sys.debug_cache();

    // 访问一个新的页page2
    println!("2--------------------------------------");
    cache_sys.add_cache_resource(&page2, buffer2);
    tmp_cid = cache_sys.query_cache_index(&page2).unwrap();
    cache_sys.get_cache_resource(tmp_cid);
    cache_sys.debug_cache(); // 测试lru是否正确地将最近访问的页面提到最前面

    // 访问page4，观察是否将page3置换
    println!("3--------------------------------------");
    cache_sys.add_cache_resource(&page4, buffer4);
    tmp_cid = cache_sys.query_cache_index(&page4).unwrap();
    cache_sys.get_cache_resource(tmp_cid);
    cache_sys.debug_cache(); // expect: 留存有1,2,4
    //
    // 模拟页面重复访问
    println!("4--------------------------------------");
    tmp_cid = cache_sys.query_cache_index(&page1).unwrap();
    cache_sys.get_cache_resource(tmp_cid);
    tmp_cid = cache_sys.query_cache_index(&page2).unwrap();
    cache_sys.get_cache_resource(tmp_cid);
    // 在访问1,2号页面后，理论上应该将page4置换
    cache_sys.add_cache_resource(&page5, buffer4);
    tmp_cid = cache_sys.query_cache_index(&page5).unwrap();
    cache_sys.get_cache_resource(tmp_cid);
    cache_sys.debug_cache();
}
