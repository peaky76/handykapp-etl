from processors.database_processors.id_hook import IdHook


def test_id_hook_is_empty_str_by_default():
    hook = IdHook()
    assert hook.val == ''

def test_id_hook_sets_itself_when_called():
    hook = IdHook()
    hook('abc')
    assert hook.val == 'abc'
