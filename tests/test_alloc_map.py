from decimal import Decimal, ROUND_DOWN

import attr
import pytest
import random

from ai.backend.agent.resources import (
    AbstractComputeDevice,
    DeviceSlotInfo,
    DiscretePropertyAllocMap,
    FractionAllocMap, FractionAllocationStrategy,
)
from ai.backend.agent.exception import (
    InsufficientResource,
    InvalidResourceArgument,
    InvalidResourceCombination, NotMultipleOfQuantum,
)
from ai.backend.common.types import (
    DeviceId,
    SlotName,
    SlotTypes,
)


@attr.s(auto_attribs=True)
class DummyDevice(AbstractComputeDevice):
    pass


def test_discrete_alloc_map():
    alloc_map = DiscretePropertyAllocMap(
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1)),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1)),
        },
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
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(100)),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(100)),
        },
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
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
        },
        allocation_strategy=FractionAllocationStrategy.FILL,
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
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
            DeviceId('a2'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
            DeviceId('a3'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
            DeviceId('a4'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
            DeviceId('a5'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
            DeviceId('a6'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
            DeviceId('a7'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
        },
        allocation_strategy=FractionAllocationStrategy.FILL,
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
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
        },
        allocation_strategy=FractionAllocationStrategy.FILL,
        quantum_size=Decimal("0.00001")
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
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.0)),
        },
        allocation_strategy=FractionAllocationStrategy.FILL,
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
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(0.05)),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(0.1)),
            DeviceId('a2'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(0.2)),
            DeviceId('a3'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(0.3)),
            DeviceId('a4'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(0.0)),
        },
        allocation_strategy=FractionAllocationStrategy.EVENLY,
    )
    for idx in range(5):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    with pytest.raises(InsufficientResource):
        alloc_map.allocate({
            SlotName('x'): Decimal('0.66'),
        })

    with pytest.raises(InsufficientResource):
        alloc_map.allocate({
            SlotName('x'): Decimal('0.06'),
        }, min_memory=Decimal(0.6))
    for _ in range(20):
        alloc_map.allocate({
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

    result = alloc_map.allocate({
        SlotName('x'): Decimal('0.2')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.2')

    alloc_map.free(result)
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0')

    result = alloc_map.allocate({
        SlotName('x'): Decimal('0.2')
    }, min_memory=Decimal('0.25'))
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('0.2')
    alloc_map.free(result)
    for idx in range(5):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate({
        SlotName('x'): Decimal('0.5')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.2')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('0.3')
    alloc_map.free(result)
    for idx in range(5):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate({
        SlotName('x'): Decimal('0.65')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0.05')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.1')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.2')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('0.3')
    alloc_map.free(result)
    for idx in range(5):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate({
        SlotName('x'): Decimal('0.6')
    }, min_memory=Decimal('0.1'))
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.1')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.2')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('0.3')
    alloc_map.free(result)
    for idx in range(5):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    alloc_map = FractionAllocMap(
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('0.3')),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('0.3')),
            DeviceId('a2'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('0.9')),
        },
    )
    result = alloc_map.allocate({
        SlotName('x'): Decimal('1')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0.3')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.3')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.4')


def test_fraction_alloc_map_even_allocation_fractions():
    alloc_map = FractionAllocMap(
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('0.8')),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('0.75')),
            DeviceId('a2'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('0.7')),
            DeviceId('a3'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('0.3')),
            DeviceId('a4'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('0.0')),
        },
        allocation_strategy=FractionAllocationStrategy.EVENLY,
    )
    result = alloc_map.allocate({
        SlotName('x'): Decimal('2.31')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal('0.67')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('0.67')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('0.67')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('0.3')
    alloc_map.free(result)
    for idx in range(4):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate({
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
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(2)),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(3)),
            DeviceId('a2'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(3)),
            DeviceId('a3'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(5)),
        },
        allocation_strategy=FractionAllocationStrategy.EVENLY,
    )
    result = alloc_map.allocate({
        SlotName('x'): Decimal('6')
    })
    assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal('3')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a2')] == Decimal('3')
    alloc_map.free(result)
    for idx in range(4):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    alloc_map = FractionAllocMap(
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1)),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1.5)),
            DeviceId('a2'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(2)),
            DeviceId('a3'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(3)),
            DeviceId('a4'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(3)),
            DeviceId('a5'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(4)),
            DeviceId('a6'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(4.5)),
            DeviceId('a7'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(5)),
            DeviceId('a8'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(5)),
        },
        allocation_strategy=FractionAllocationStrategy.EVENLY,
    )

    result = alloc_map.allocate({
        SlotName('x'): Decimal('6')
    }, min_memory=Decimal('2.5'))
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('3')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a4')] == Decimal('3')
    alloc_map.free(result)
    for idx in range(9):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')

    result = alloc_map.allocate({
        SlotName('x'): Decimal('11')
    }, min_memory=Decimal('0.84'))
    assert alloc_map.allocations[SlotName('x')][DeviceId('a3')] == Decimal('2.75')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a4')] == Decimal('2.75')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a5')] == Decimal('2.75')
    assert alloc_map.allocations[SlotName('x')][DeviceId('a5')] == Decimal('2.75')
    alloc_map.free(result)
    for idx in range(9):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')


def test_fraction_alloc_map_even_allocation_many_devices_2():
    alloc_map = FractionAllocMap(
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('1.0')),
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('1.0')),
            DeviceId('a2'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('1.0')),
            DeviceId('a3'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('1.0')),
            DeviceId('a4'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('1.0')),
            DeviceId('a5'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('1.0')),
            DeviceId('a6'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('1.0')),
            DeviceId('a7'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal('1.0')),
        },
        allocation_strategy=FractionAllocationStrategy.EVENLY,
    )
    result = alloc_map.allocate({
        SlotName('x'): Decimal('6')
    })
    count_0 = 0
    count_1 = 0
    # NOTE: the even allocator favors the tail of device list when it fills up.
    # So we rely on the counting of desire per-device allocations instead of matching
    # the device index and the allocations.
    for idx in range(8):
        if alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('1.0'):
            count_1 += 1
        if alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0'):
            count_0 += 1
    assert count_0 == 2
    assert count_1 == 6
    alloc_map.free(result)
    for idx in range(8):
        assert alloc_map.allocations[SlotName('x')][DeviceId(f'a{idx}')] == Decimal('0')


@pytest.mark.parametrize(
    "alloc_strategy",
    [FractionAllocationStrategy.FILL, FractionAllocationStrategy.EVENLY],
)
def test_quantum_size(alloc_strategy):
    alloc_map = FractionAllocMap(
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1)),  # noqa
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1)),  # noqa
        },
        quantum_size=Decimal("0.25"),
        allocation_strategy=alloc_strategy,
    )
    result = alloc_map.allocate({
        SlotName('x'): Decimal("0.5"),
    })
    assert sum(alloc_map.allocations[SlotName('x')].values()) == Decimal("0.5")
    alloc_map.free(result)

    result = alloc_map.allocate({
        SlotName('x'): Decimal("1.5"),
    })
    assert sum(alloc_map.allocations[SlotName('x')].values()) == Decimal("1.5")
    if alloc_strategy == FractionAllocationStrategy.EVENLY:
        assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal("0.75")
        assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal("0.75")
    else:
        assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal("1.00")
        assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal("0.50")
    alloc_map.free(result)

    # inputs are not multiple of 0.25
    with pytest.raises(NotMultipleOfQuantum):
        alloc_map.allocate({
            SlotName('x'): Decimal("0.52"),
        })
    with pytest.raises(NotMultipleOfQuantum):
        alloc_map.allocate({
            SlotName('x'): Decimal("0.42"),
        })
    with pytest.raises(NotMultipleOfQuantum):
        alloc_map.allocate({
            SlotName('x'): Decimal("3.99"),
        })

    if alloc_strategy == FractionAllocationStrategy.EVENLY:
        # input IS multiple of 0.25 but the CALCULATED allocations are not multiple of 0.25
        with pytest.raises(InsufficientResource, match="not a multiple of"):
            alloc_map.allocate({
                SlotName('x'): Decimal("1.75"),  # divided to 0.88 and 0.87
            })
    else:
        # In this case, it satisfies the quantum condition, because the capacity of devices are
        # multiples of the quantum.
        alloc_map.allocate({
            SlotName('x'): Decimal("1.75"),
        })
        assert alloc_map.allocations[SlotName('x')][DeviceId('a0')] == Decimal("1.00")
        assert alloc_map.allocations[SlotName('x')][DeviceId('a1')] == Decimal("0.75")

        # So let's change the situation.
        alloc_map = FractionAllocMap(
            device_slots={
                DeviceId('a0'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1)),  # noqa
                DeviceId('a1'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('x'), Decimal(1)),  # noqa
            },
            quantum_size=Decimal("0.3"),
            allocation_strategy=alloc_strategy,
        )
        with pytest.raises(NotMultipleOfQuantum):
            alloc_map.allocate({
                SlotName('x'): Decimal("0.5"),
            })
        with pytest.raises(InsufficientResource, match="not a multiple of"):
            alloc_map.allocate({
                SlotName('x'): Decimal("1.2"),
            })


def test_exclusive_resource_slots():
    alloc_map = DiscretePropertyAllocMap(
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.UNIQUE, SlotName('cuda.device:1g.5gb-mig'), Decimal(1)),  # noqa
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.UNIQUE, SlotName('cuda.device:1g.5gb-mig'), Decimal(1)),  # noqa
            DeviceId('a2'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('cuda.device'), Decimal(1)),
            DeviceId('a3'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('cuda.device'), Decimal(1)),
            DeviceId('a4'): DeviceSlotInfo(SlotTypes.UNIQUE, SlotName('cuda.device:3g.20gb-mig'), Decimal(1)),  # noqa
        },
        exclusive_slot_types={'cuda.device:*-mig', 'cuda.device', 'cuda.shares'},
    )

    def check_clean():
        assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a0')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a1')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.device')][DeviceId('a2')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.device')][DeviceId('a3')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.device:3g.20gb-mig')][DeviceId('a4')] == Decimal('0')

    with pytest.raises(InvalidResourceCombination):
        alloc_map.allocate({
            SlotName('cuda.device'): Decimal('2'),
            SlotName('cuda.device:1g.5gb-mig'): Decimal('1'),
        })
    check_clean()


def test_heterogeneous_resource_slots_with_discrete_alloc_map():
    alloc_map = DiscretePropertyAllocMap(
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.UNIQUE, SlotName('cuda.device:1g.5gb-mig'), Decimal(1)),  # noqa
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.UNIQUE, SlotName('cuda.device:1g.5gb-mig'), Decimal(1)),  # noqa
            DeviceId('a2'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('cuda.device'), Decimal(1)),
            DeviceId('a3'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('cuda.device'), Decimal(1)),
            DeviceId('a4'): DeviceSlotInfo(SlotTypes.UNIQUE, SlotName('cuda.device:3g.20gb-mig'), Decimal(1)),  # noqa
        },
        exclusive_slot_types={'cuda.device:*-mig', 'cuda.device', 'cuda.shares'},
    )

    def check_clean():
        assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a0')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a1')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.device')][DeviceId('a2')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.device')][DeviceId('a3')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.device:3g.20gb-mig')][DeviceId('a4')] == Decimal('0')

    check_clean()

    # check allocation of non-unique slots
    result = alloc_map.allocate({
        SlotName('cuda.device'): Decimal('2')
    })
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a0')] == Decimal('0')
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a1')] == Decimal('0')
    assert alloc_map.allocations[SlotName('cuda.device')][DeviceId('a2')] == Decimal('1')
    assert alloc_map.allocations[SlotName('cuda.device')][DeviceId('a3')] == Decimal('1')
    assert alloc_map.allocations[SlotName('cuda.device:3g.20gb-mig')][DeviceId('a4')] == Decimal('0')
    alloc_map.free(result)
    check_clean()

    with pytest.raises(InsufficientResource):
        alloc_map.allocate({
            SlotName('cuda.device'): Decimal('3')
        })
    check_clean()

    # allocating zero means no-op.
    alloc_map.allocate({
        SlotName('cuda.device:1g.5gb-mig'): Decimal('0')
    })
    check_clean()

    # any allocation request for unique slots should specify the amount 1.
    with pytest.raises(InvalidResourceArgument):
        alloc_map.allocate({
            SlotName('cuda.device:1g.5gb-mig'): Decimal('1.1')
        })
    with pytest.raises(InvalidResourceArgument):
        alloc_map.allocate({
            SlotName('cuda.device:1g.5gb-mig'): Decimal('2')
        })
    check_clean()

    # test alloaction of unique slots
    result1 = alloc_map.allocate({
        SlotName('cuda.device:1g.5gb-mig'): Decimal('1')
    })
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a0')] == Decimal('1')
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a1')] == Decimal('0')
    result2 = alloc_map.allocate({
        SlotName('cuda.device:1g.5gb-mig'): Decimal('1')
    })
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a0')] == Decimal('1')
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a1')] == Decimal('1')
    with pytest.raises(InsufficientResource):
        alloc_map.allocate({
            SlotName('cuda.device:1g.5gb-mig'): Decimal('1')
        })
    alloc_map.free(result1)
    alloc_map.free(result2)
    check_clean()


def test_heterogeneous_resource_slots_with_fractional_alloc_map():
    alloc_map = FractionAllocMap(
        device_slots={
            DeviceId('a0'): DeviceSlotInfo(SlotTypes.UNIQUE, SlotName('cuda.device:1g.5gb-mig'), Decimal(1)),  # noqa
            DeviceId('a1'): DeviceSlotInfo(SlotTypes.UNIQUE, SlotName('cuda.device:1g.5gb-mig'), Decimal(1)),  # noqa
            DeviceId('a2'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('cuda.shares'), Decimal('1.0')),
            DeviceId('a3'): DeviceSlotInfo(SlotTypes.COUNT, SlotName('cuda.shares'), Decimal('1.0')),
            DeviceId('a4'): DeviceSlotInfo(SlotTypes.UNIQUE, SlotName('cuda.device:3g.20gb-mig'), Decimal(1)),  # noqa
        },
        exclusive_slot_types={'cuda.device:*-mig', 'cuda.device', 'cuda.shares'},
        allocation_strategy=FractionAllocationStrategy.FILL,
    )

    def check_clean():
        assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a0')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a1')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.shares')][DeviceId('a2')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.shares')][DeviceId('a3')] == Decimal('0')
        assert alloc_map.allocations[SlotName('cuda.device:3g.20gb-mig')][DeviceId('a4')] == Decimal('0')

    check_clean()

    # check allocation of non-unique slots
    result = alloc_map.allocate({
        SlotName('cuda.shares'): Decimal('2.0')
    })
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a0')] == Decimal('0')
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a1')] == Decimal('0')
    assert alloc_map.allocations[SlotName('cuda.shares')][DeviceId('a2')] == Decimal('1.0')
    assert alloc_map.allocations[SlotName('cuda.shares')][DeviceId('a3')] == Decimal('1.0')
    assert alloc_map.allocations[SlotName('cuda.device:3g.20gb-mig')][DeviceId('a4')] == Decimal('0')
    alloc_map.free(result)
    check_clean()

    with pytest.raises(InsufficientResource):
        alloc_map.allocate({
            SlotName('cuda.shares'): Decimal('2.5')
        })
    check_clean()

    # allocating zero means no-op.
    alloc_map.allocate({
        SlotName('cuda.device:1g.5gb-mig'): Decimal('0')
    })
    check_clean()

    # any allocation request for unique slots should specify the amount 1.
    with pytest.raises(InvalidResourceArgument):
        alloc_map.allocate({
            SlotName('cuda.device:1g.5gb-mig'): Decimal('0.3')
        })
    with pytest.raises(InvalidResourceArgument):
        alloc_map.allocate({
            SlotName('cuda.device:1g.5gb-mig'): Decimal('1.5')
        })
    check_clean()

    # test alloaction of unique slots
    result1 = alloc_map.allocate({
        SlotName('cuda.device:1g.5gb-mig'): Decimal('1')
    })
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a0')] == Decimal('1')
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a1')] == Decimal('0')
    result2 = alloc_map.allocate({
        SlotName('cuda.device:1g.5gb-mig'): Decimal('1')
    })
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a0')] == Decimal('1')
    assert alloc_map.allocations[SlotName('cuda.device:1g.5gb-mig')][DeviceId('a1')] == Decimal('1')
    with pytest.raises(InsufficientResource):
        alloc_map.allocate({
            SlotName('cuda.device:1g.5gb-mig'): Decimal('1')
        })
    alloc_map.free(result1)
    alloc_map.free(result2)
    check_clean()
