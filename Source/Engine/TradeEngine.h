#pragma once

class TradeEngine
{
public:
	static TradeEngine& Instance();

	void Init();
	void Destroy();

protected:
	TradeEngine() = default;
	TradeEngine(const TradeEngine& rhs) = delete;
	TradeEngine(TradeEngine&& rhs) = delete;
	TradeEngine& operator=(const TradeEngine& rhs) = delete;
	TradeEngine& operator=(TradeEngine&& rhs) = delete;

private:

};