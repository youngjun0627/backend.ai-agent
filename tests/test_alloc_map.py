from decimal import Decimal, ROUND_DOWN

import attr
import pytest
import random

from ai.backend.agent.resources import (
    AbstractComputeDevice,
    DiscretePropertyAllocMap,
    FractionAllocMap,
)
from ai.backend.agent.exception import InsufficientResource
from ai.backend.common.types import (
    DeviceId,
    SlotName,
)


@attr.s(auto_attribs=True)
class DummyDevice(AbstractComputeDevice):
    pass


def test_discrete_alloc_map():
    alloc_map = DiscretePropertyAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
        ],
        prop_func=lambda dev: 1,
    )
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == 0
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == 0

    result = alloc_map.allocate({
        SlotName('x'): Decimal('1'),
    })
    assert result[SlotName('x')][DeviceId('a0')] == 1
    assert DeviceId('a1') not in result[SlotName('x')]
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == 1
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == 0

    with pytest.raises(InsufficientResource):
        alloc_map.allocate({
            SlotName('x'): Decimal('3'),
        })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == 1
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == 0

    alloc_map.free(result)
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == 0
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == 0


def test_discrete_alloc_map_large_number():
    alloc_map = DiscretePropertyAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
        ],
        prop_func=lambda dev: 100,
    )
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == 0
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == 0

    result = alloc_map.allocate({
        SlotName('x'): Decimal('130'),
    })
    assert result[SlotName('x')][DeviceId('a0')] == 100
    assert result[SlotName('x')][DeviceId('a1')] == 30
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == 100
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == 30

    with pytest.raises(InsufficientResource):
        alloc_map.allocate({
            SlotName('x'): Decimal('71'),
        })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == 100
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == 30

    alloc_map.free(result)
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == 0
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == 0


def test_fraction_alloc_map():
    alloc_map = FractionAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
        ],
        shares_per_device={
            DeviceId('a0'): Decimal('1.0'),
            DeviceId('a1'): Decimal('1.0'),
        },
    )
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0')

    result = alloc_map.allocate({
        SlotName('x'): Decimal('1.5'),
    })
    assert result[SlotName('x')][DeviceId('a0')] == Decimal('1.0')
    assert result[SlotName('x')][DeviceId('a1')] == Decimal('0.5')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('1.0')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.5')

    with pytest.raises(InsufficientResource):
        alloc_map.allocate({
            SlotName('x'): Decimal('1.5'),
        })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('1.0')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.5')

    alloc_map.free(result)
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0')


def test_fraction_alloc_map_many_device():
    alloc_map = FractionAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
            DummyDevice(DeviceId('a2'), 'bus-02', 0, 1, 1),
            DummyDevice(DeviceId('a3'), 'bus-03', 0, 1, 1),
            DummyDevice(DeviceId('a4'), 'bus-04', 0, 1, 1),
            DummyDevice(DeviceId('a5'), 'bus-05', 0, 1, 1),
            DummyDevice(DeviceId('a6'), 'bus-06', 0, 1, 1),
            DummyDevice(DeviceId('a7'), 'bus-07', 0, 1, 1),
        ],
        shares_per_device={
            DeviceId('a0'): Decimal('1.0'),
            DeviceId('a1'): Decimal('1.0'),
            DeviceId('a2'): Decimal('1.0'),
            DeviceId('a3'): Decimal('1.0'),
            DeviceId('a4'): Decimal('1.0'),
            DeviceId('a5'): Decimal('1.0'),
            DeviceId('a6'): Decimal('1.0'),
            DeviceId('a7'): Decimal('1.0'),
        },
    )
    for idx in range(8):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate({
        SlotName('x'): Decimal('7.95'),
    })
    for idx in range(7):
        assert result[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('1.0')
    assert result[SlotName('x')][DeviceId('a7')] == Decimal('0.95')
    for idx in range(7):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('1.0')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a7')] == Decimal('0.95')

    with pytest.raises(InsufficientResource):
        alloc_map.allocate({
            SlotName('x'): Decimal('1.0'),
        })
    for idx in range(7):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('1.0')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a7')] == Decimal('0.95')

    alloc_map.free(result)
    for idx in range(8):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')


def test_fraction_alloc_map_iteration():
    alloc_map = FractionAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
        ],
        shares_per_device={
            DeviceId('a0'): Decimal('1.0'),
            DeviceId('a1'): Decimal('1.0'),
        },
    )
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0')

    for _ in range(1000):
        alloc_map.allocate({
            SlotName('x'): Decimal('0.00001'),
        })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0.005')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.005')

    alloc_map.free({SlotName('x'): {DeviceId('a0'): Decimal('0.00001')}})
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0.00499')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.005')

    for _ in range(499):
        alloc_map.free({SlotName('x'): {DeviceId('a0'): Decimal('0.00001')}})
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.005')


def test_fraction_alloc_map_random_generated_allocations():
    alloc_map = FractionAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
        ],
        shares_per_device={
            DeviceId('a0'): Decimal('1.0'),
            DeviceId('a1'): Decimal('1.0'),
        },
    )
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0')

    quantum = Decimal('.01')
    for _ in range(5):
        allocations = []
        for _ in range(10):
            result = alloc_map.allocate({
                SlotName('x'): Decimal(random.uniform(0, 0.1)).quantize(quantum, ROUND_DOWN),
            })
            allocations.append(result)
        assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] >= Decimal('0')
        assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] >= Decimal('0')
        for a in allocations:
            alloc_map.free(a)
        assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0')
        assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0')


def test_fraction_alloc_map_even_allocation():
    alloc_map = FractionAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
            DummyDevice(DeviceId('a2'), 'bus-02', 0, 1, 1),
            DummyDevice(DeviceId('a3'), 'bus-03', 0, 1, 1),
            DummyDevice(DeviceId('a4'), 'bus-04', 0, 1, 1),
        ],
        shares_per_device={
            DeviceId('a0'): Decimal('0.05'),
            DeviceId('a1'): Decimal('0.1'),
            DeviceId('a2'): Decimal('0.2'),
            DeviceId('a3'): Decimal('0.3'),
            DeviceId('a4'): Decimal('0.0'),
        },
    )
    for idx in range(5):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    with pytest.raises(InsufficientResource):
        alloc_map.allocate_evenly({
            SlotName('x'): Decimal('0.66'),
        })

    with pytest.raises(InsufficientResource):
        alloc_map.allocate_evenly({
            SlotName('x'): Decimal('0.06'),
        }, min_memory=Decimal(0.6))
    for _ in range(20):
        alloc_map.allocate_evenly({
            SlotName('x'): Decimal('0.01'),
        })

    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0.05')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.1')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.05')
    alloc_map.free({SlotName('x'): {DeviceId('a0'): Decimal('0.05'),
                                    DeviceId('a1'): Decimal('0.1'),
                                    DeviceId('a2'): Decimal('0.05')}})
    for idx in range(0):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('0.2')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.2')

    alloc_map.free(result)
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0')

    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('0.2')
    }, min_memory=Decimal('0.25'))
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('0.2')
    alloc_map.free(result)
    for idx in range(5):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('0.5')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.2')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('0.3')
    alloc_map.free(result)
    for idx in range(5):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('0.65')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0.05')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.1')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.2')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('0.3')
    alloc_map.free(result)
    for idx in range(5):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('0.6')
    }, min_memory=Decimal('0.1'))
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.1')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.2')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('0.3')
    alloc_map.free(result)
    for idx in range(5):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    alloc_map = FractionAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
            DummyDevice(DeviceId('a2'), 'bus-02', 0, 1, 1)
        ],
        shares_per_device={
            DeviceId('a0'): Decimal('0.3'),
            DeviceId('a1'): Decimal('0.3'),
            DeviceId('a2'): Decimal('0.9')
        }
    )
    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('1')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0.3')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.3')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.4')


def test_fraction_alloc_map_even_allocation_fractions():
    alloc_map = FractionAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
            DummyDevice(DeviceId('a2'), 'bus-02', 0, 1, 1),
            DummyDevice(DeviceId('a3'), 'bus-03', 0, 1, 1),
            DummyDevice(DeviceId('a4'), 'bus-04', 0, 1, 1),
        ],
        shares_per_device={
            DeviceId('a0'): Decimal('0.8'),
            DeviceId('a1'): Decimal('0.75'),
            DeviceId('a2'): Decimal('0.7'),
            DeviceId('a3'): Decimal('0.3'),
            DeviceId('a4'): Decimal('0.0'),
        },
    )
    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('2.31')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0.67')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.67')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.67')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('0.3')
    alloc_map.free(result)
    for idx in range(4):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('2')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0.67')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.67')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.66')
    alloc_map.free(result)
    for idx in range(3):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')


def test_fraction_alloc_map_even_allocation_many_devices():
    alloc_map = FractionAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
            DummyDevice(DeviceId('a2'), 'bus-02', 0, 1, 1),
            DummyDevice(DeviceId('a3'), 'bus-03', 0, 1, 1)
        ],
        shares_per_device={
            DeviceId('a0'): Decimal('2'),
            DeviceId('a1'): Decimal('3'),
            DeviceId('a2'): Decimal('3'),
            DeviceId('a3'): Decimal('5')
        },
    )
    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('6')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('3')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('3')
    alloc_map.free(result)
    for idx in range(4):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    alloc_map = FractionAllocMap(
        devices=[
            DummyDevice(DeviceId('a0'), 'bus-00', 0, 1, 1),
            DummyDevice(DeviceId('a1'), 'bus-01', 0, 1, 1),
            DummyDevice(DeviceId('a2'), 'bus-02', 0, 1, 1),
            DummyDevice(DeviceId('a3'), 'bus-03', 0, 1, 1),
            DummyDevice(DeviceId('a4'), 'bus-03', 0, 1, 1),
            DummyDevice(DeviceId('a5'), 'bus-03', 0, 1, 1),
            DummyDevice(DeviceId('a6'), 'bus-03', 0, 1, 1),
            DummyDevice(DeviceId('a7'), 'bus-03', 0, 1, 1),
            DummyDevice(DeviceId('a8'), 'bus-03', 0, 1, 1)
        ],
        shares_per_device={
            DeviceId('a0'): Decimal('1'),
            DeviceId('a1'): Decimal('1.5'),
            DeviceId('a2'): Decimal('2'),
            DeviceId('a3'): Decimal('3'),
            DeviceId('a4'): Decimal('3'),
            DeviceId('a5'): Decimal('4'),
            DeviceId('a6'): Decimal('4.5'),
            DeviceId('a7'): Decimal('5'),
            DeviceId('a8'): Decimal('5')
        },
    )

    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('6')
    }, min_memory=Decimal('2.5'))
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('3')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a4')] == Decimal('3')
    alloc_map.free(result)
    for idx in range(9):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate_evenly({
        SlotName('x'): Decimal('11')
    }, min_memory=Decimal('0.84'))
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('2.75')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a4')] == Decimal('2.75')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a5')] == Decimal('2.75')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a5')] == Decimal('2.75')
    alloc_map.free(result)
    for idx in range(9):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')
