package db_partitions_viewer

import (
	"database/sql"
	"fmt"
	"strconv"
)

const mysqlRangePartitionType = "RANGE"
const mysqlMaxValue = "MAXVALUE"

type DbPartitionsViewer struct {
	db *sql.DB
}

func (p RangePartitionType) String() string {
	if p.To == nil {
		return fmt.Sprintf("[%d-MAXVALUE]", p.From)
	}
	return fmt.Sprintf("[%d-%d]", p.From, *p.To)
}

type RangePartitionType struct {
	From uint64
	To   *uint64
}

func NewDbPartitionsViewer(db *sql.DB) *DbPartitionsViewer {
	viewer := new(DbPartitionsViewer)
	viewer.db = db
	return viewer
}

func (pv DbPartitionsViewer) GetHoldingMarketOfferPartitionsByHoldingId() ([]RangePartitionType, error) {
	return pv.getRangePartitionInfo("holding_market_offer")
}

func (pv DbPartitionsViewer) GetHoldingMarketOfferInteractionPartitionsByHoldingId() ([]RangePartitionType, error) {
	return pv.getRangePartitionInfo("holding_market_offer_interaction")
}

func (pv DbPartitionsViewer) GetVehicleSaleStatisticViewPartitionsByHoldingId() ([]RangePartitionType, error) {
	return pv.getRangePartitionInfo("vehicle_sale_statistic_view")
}

func (pv DbPartitionsViewer) getRangePartitionInfo(tableName string) ([]RangePartitionType, error) {
	const q = `
SELECT 
    PARTITION_METHOD,
    PARTITION_DESCRIPTION
FROM information_schema.PARTITIONS 
WHERE TABLE_SCHEMA = 'maxposter' 
      AND TABLE_NAME = '%s'
ORDER BY PARTITION_ORDINAL_POSITION ASC
`
	rows, err := pv.db.Query(fmt.Sprintf(q, tableName))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	ranges := make([]RangePartitionType, 0)
	var startKey uint64
	for rows.Next() {
		var partitionMethod string
		var partitionDescription string
		err = rows.Scan(
			&partitionMethod,
			&partitionDescription,
		)
		if err != nil {
			return nil, err
		}
		if partitionMethod != mysqlRangePartitionType {
			return nil, fmt.Errorf("Неизвестный тип партиционирования")
		}
		value, err := pv.getValByPartitionDescription(partitionDescription)
		if err != nil {
			return nil, err
		}
		rangeItem := new(RangePartitionType)
		rangeItem.From = startKey
		if value != nil {
			tmpEndVal := *value - 1
			rangeItem.To = &tmpEndVal
			startKey = *value
		}
		ranges = append(ranges, *rangeItem)
	}
	return ranges, nil
}

func (pv DbPartitionsViewer) getValByPartitionDescription(partitionDescription string) (*uint64, error) {
	var val *uint64
	if partitionDescription != mysqlMaxValue {
		tmpVal, err := strconv.ParseUint(partitionDescription, 0, 64)
		if err != nil {
			return val, err
		}
		val = &tmpVal
	}
	return val, nil
}
