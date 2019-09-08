import pytest

from ai.backend.agent.kernel import (
    match_krunner_volume,
)


def test_match_krunner_volume():
    krunner_volumes = {
        'ubuntu8.04': 'u1',
        'ubuntu18.04': 'u2',
        'centos7.6': 'c1',
        'centos8.0': 'c2',
        'centos5.0': 'c3',
    }

    ret = match_krunner_volume(krunner_volumes, 'centos7.6')
    assert ret[0] == 'centos7.6'
    assert ret[1] == 'c1'

    ret = match_krunner_volume(krunner_volumes, 'centos8.0')
    assert ret[0] == 'centos8.0'
    assert ret[1] == 'c2'

    ret = match_krunner_volume(krunner_volumes, 'centos')
    assert ret[0] == 'centos8.0'
    assert ret[1] == 'c2'

    ret = match_krunner_volume(krunner_volumes, 'ubuntu18.04')
    assert ret[0] == 'ubuntu18.04'
    assert ret[1] == 'u2'

    ret = match_krunner_volume(krunner_volumes, 'ubuntu')
    assert ret[0] == 'ubuntu18.04'
    assert ret[1] == 'u2'

    with pytest.raises(RuntimeError):
        match_krunner_volume(krunner_volumes, 'ubnt')

    with pytest.raises(RuntimeError):
        match_krunner_volume(krunner_volumes, 'xyz')
