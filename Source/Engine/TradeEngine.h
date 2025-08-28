#pragma once

#include "FutureType.h"

class TradeEngine
{
public:
	static TradeEngine& Instance();

	void Init();
	void Destroy();

	FutureID ParseFutureID(const std::string& str);
	const std::string& ToFutureIDStr(const FutureID id);

protected:
	TradeEngine() = default;
	TradeEngine(const TradeEngine& rhs) = delete;
	TradeEngine(TradeEngine&& rhs) = delete;
	TradeEngine& operator=(const TradeEngine& rhs) = delete;
	TradeEngine& operator=(TradeEngine&& rhs) = delete;

private:

};