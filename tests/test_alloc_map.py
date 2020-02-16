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
