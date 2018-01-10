package com.sirolf2009.seer

import com.sirolf2009.commonwealth.trading.ITrade
import java.io.File
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.ArrayList
import java.util.Date
import java.util.LinkedList
import java.util.Queue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import org.eclipse.xtend.lib.annotations.Data
import org.eclipse.xtend.lib.annotations.FinalFieldsConstructor
import tech.tablesaw.api.CategoryColumn
import tech.tablesaw.api.DateTimeColumn
import tech.tablesaw.api.DoubleColumn
import tech.tablesaw.api.Table
import tech.tablesaw.api.plot.Scatter

import static extension com.sirolf2009.seer.retrieval.DataParser.*
import static tech.tablesaw.api.QueryHelper.*;

class Walls {
	
	static val location = "walls.csv"

	def static void main(String[] args) {
		val table = Table.read.csv(location).selectWhere(allOf(column("contactType").isEqualTo("HELD"), column("wallType").isEqualTo("BID")))
		val dateColumn = table.dateTimeColumn("timestamp")
		val wallTypeColumn = table.categoryColumn("wallType")
		val detectedColumn = table.dateTimeColumn("detected")
		val priceColumn = table.floatColumn("price")
		val sizeColumn = table.floatColumn("size")
		val contactTypeColumn = table.categoryColumn("contactType")
		val currentSizeColumn = table.floatColumn("currentSize")
		val buyVolumeColumn = table.floatColumn("buyVolume")
		val sellVolumeColumn = table.floatColumn("sellVolume")
		
		Scatter.show("BID HELD WALLS", sizeColumn, buyVolumeColumn)
	}
	
	def static createDataset() {
		val contacts = new ArrayList()
		val bidWall = new AtomicReference()
		val bidWallTouching = new AtomicBoolean(false)
		val askWall = new AtomicReference()
		val askWallTouching = new AtomicBoolean(false)
		val tradeTracker = new TradeTracker(Duration.ofMinutes(1).toMillis())
		new File("/home/sirolf2009/Downloads/2018/0").parseFolder.forEach [
			trades.forEach[tradeTracker.add(it)]
			{
				val bidOrder = orderbook.bids.get(0)
				val bidSize = bidOrder.amount.doubleValue()
				val bidPrice = bidOrder.price.doubleValue()
				val Wall wall = bidWall.get()
				if(bidSize > 100) {
					val newWall = new Wall(WallType.BID, bidPrice, bidSize, timestamp)
					if(wall === null || newWall.price != wall.price) {
						if(wall !== null) {
							println(timestamp + " Bid wall held\t" + wall + "\t" + bidPrice + " " + bidSize + " " + tradeTracker.sumBuy() + " " + tradeTracker.sumSell())
							contacts.add(new WallContact(wall, ContactType.HELD, bidSize, tradeTracker.sumBuy(), tradeTracker.sumSell(), timestamp))
						}
						println(timestamp + " Bid wall detected\t" + newWall + "\t" + bidPrice + " " + bidSize + " " + tradeTracker.sumBuy() + " " + tradeTracker.sumSell())
						contacts.add(new WallContact(newWall, ContactType.DETECTED, bidSize, tradeTracker.sumBuy(), tradeTracker.sumSell(), timestamp))
						bidWall.set(newWall)
						bidWallTouching.set(true)
					}
				} else if(wall !== null) {
					if(wall.price > bidPrice) {
						println(timestamp + " Bid wall breached\t" + wall + "\t" + bidPrice + " " + bidSize + " " + tradeTracker.sumBuy() + " " + tradeTracker.sumSell())
						contacts.add(new WallContact(wall, ContactType.BREACHED, 0, tradeTracker.sumBuy(), tradeTracker.sumSell(), timestamp))
						bidWall.set(null)
					} else if(bidWallTouching.get() && wall.price < bidPrice) {
						println(timestamp + " Bid wall bounced\t" + wall + "\t" + bidPrice + " " + bidSize + " " + tradeTracker.sumBuy() + " " + tradeTracker.sumSell())
						contacts.add(new WallContact(wall, ContactType.BOUNCED, bidSize, tradeTracker.sumBuy(), tradeTracker.sumSell(), timestamp))
						bidWallTouching.set(false)
					}
				}
			}
			{
				val askOrder = orderbook.asks.get(0)
				val askSize = askOrder.amount.doubleValue()
				val askPrice = askOrder.price.doubleValue()
				val Wall wall = askWall.get()
				if(askSize > 100) {
					val newWall = new Wall(WallType.ASK, askPrice, askSize, timestamp)
					if(wall === null || newWall.price != wall.price) {
						if(wall !== null) {
							println(timestamp + " Ask wall held\t" + wall + "\t" + askPrice + " " + askSize + " " + tradeTracker.sumBuy() + " " + tradeTracker.sumSell())
							contacts.add(new WallContact(wall, ContactType.HELD, askSize, tradeTracker.sumBuy(), tradeTracker.sumSell(), timestamp))
						}
						println(timestamp + " Ask wall detected\t" + newWall + "\t" + askPrice + " " + askSize + " " + tradeTracker.sumBuy() + " " + tradeTracker.sumSell())
						contacts.add(new WallContact(newWall, ContactType.DETECTED, askSize, tradeTracker.sumBuy(), tradeTracker.sumSell(), timestamp))
						askWall.set(newWall)
						askWallTouching.set(true)
					}
				} else if(wall !== null) {
					if(wall.price < askPrice) {
						println(timestamp + " Ask wall breached\t" + wall + "\t" + askPrice + " " + askSize + " " + tradeTracker.sumBuy() + " " + tradeTracker.sumSell())
						contacts.add(new WallContact(wall, ContactType.BREACHED, 0, tradeTracker.sumBuy(), tradeTracker.sumSell(), timestamp))
						askWall.set(null)
					} else if(bidWallTouching.get() && wall.price > askPrice) {
						println(timestamp + " Ask wall bounced\t" + wall + "\t" + askPrice + " " + askSize + " " + tradeTracker.sumBuy() + " " + tradeTracker.sumSell())
						contacts.add(new WallContact(wall, ContactType.BOUNCED, askSize, tradeTracker.sumBuy(), tradeTracker.sumSell(), timestamp))
						askWallTouching.set(false)
					}
				}
			}
		]
		val table = Table.create("walls")
		val dateColumn = new DateTimeColumn("timestamp")
		val wallTypeColumn = new CategoryColumn("wallType", #[WallType.BID.toString(), WallType.ASK.toString()])
		val detectedColumn = new DateTimeColumn("detected")
		val priceColumn = new DoubleColumn("price")
		val sizeColumn = new DoubleColumn("size")
		val contactTypeColumn = new CategoryColumn("contactType", #[ContactType.DETECTED.toString(), ContactType.BOUNCED.toString(), ContactType.BREACHED.toString(), ContactType.HELD.toString()])
		val currentSizeColumn = new DoubleColumn("currentSize")
		val buyVolumeColumn = new DoubleColumn("buyVolume")
		val sellVolumeColumn = new DoubleColumn("sellVolume")
		table.addColumn(dateColumn, wallTypeColumn, detectedColumn, priceColumn, sizeColumn, contactTypeColumn, currentSizeColumn, buyVolumeColumn, sellVolumeColumn)
		contacts.forEach[
			dateColumn.append(LocalDateTime.ofInstant(hit.toInstant, ZoneId.systemDefault))
			wallTypeColumn.appendCell(wall.type.toString())
			detectedColumn.append(LocalDateTime.ofInstant(wall.hit.toInstant, ZoneId.systemDefault))
			priceColumn.append(wall.price)
			sizeColumn.append(wall.amount)
			contactTypeColumn.appendCell(type.toString())
			currentSizeColumn.append(currentSize)
			buyVolumeColumn.append(buyAmount)
			sellVolumeColumn.append(sellAmount)
		]
		table.save(location)
	}

	@Data static class WallContact {
		val Wall wall
		val ContactType type
		val double currentSize
		val double buyAmount
		val double sellAmount
		val Date hit
	}

	static enum ContactType {
		DETECTED,
		BOUNCED,
		BREACHED,
		HELD
	}

	@Data static class Wall {
		val WallType type
		val double price
		val double amount
		val Date hit

		override toString() {
			return '''«hit» type=«type» price=«price» amount=«amount»'''
		}
	}

	static enum WallType {
		BID,
		ASK
	}

	@FinalFieldsConstructor static class TradeTracker {

		val Queue<ITrade> trades = new LinkedList()
		val long timeout

		def add(ITrade trade) {
			trades.add(trade)
			while(trade.point.x.longValue - trades.peek().point.x.longValue > timeout) {
				trades.poll()
			}
		}

		def sumBuy() {
			trades.stream.map[amount.doubleValue()].filter[it > 0].reduce[a, b|a + b].orElse(0d)
		}

		def sumSell() {
			trades.stream.map[amount.doubleValue()].filter[it < 0].map[-it].reduce[a, b|a + b].orElse(0d)
		}

	}

}
