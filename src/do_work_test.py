from do_work import do_work

def test_do_work():
    test_input = {}
    
    result = do_work(test_input)

    assert result['delay']
