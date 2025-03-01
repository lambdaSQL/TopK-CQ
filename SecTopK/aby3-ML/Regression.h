#pragma once

#include <iostream>
#include <math.h>
#include <vector>
#include <algorithm>
#include <Eigen/Dense>
#include <cryptoTools/Crypto/PRNG.h>
#include <cryptoTools/Common/Log.h>
//#include "Engines/Lynx/LynxDefines.h"
#include <aby3/sh3/Sh3FixedPoint.h>
using namespace oc;

#define DEBUG_PRINT(x)

struct RegressionParam
{
	u64 mIterations;
	u64 mBatchSize;
	double mLearningRate;
};


inline void getSubset(std::vector<u64>& dest, std::vector<u64>& pool, std::vector<u64>::iterator& poolIter, PRNG& prng)
{
	auto destIter = dest.begin();
	while (destIter != dest.end())
	{
		auto step = std::min<u64>(pool.end() - poolIter, dest.end() - destIter);
		std::copy(poolIter, poolIter + step, destIter);
		poolIter += step;
		destIter += step;

		if (poolIter == pool.end())
		{
			std::random_shuffle(pool.begin(), pool.end(), prng);
			poolIter = pool.begin();
		}
	}
}

template<typename Matrix>
void extractBatch(
	Matrix& XX,
	Matrix& YY,
	const Matrix& X,
	const Matrix& Y,
	const std::vector<u64>& indices)
{
	if (XX.rows() != indices.size()) throw std::runtime_error(LOCATION);

	for (u64 i = 0; i < indices.size(); ++i)
	{
		XX.row(i) = X.row(indices[i]);
		YY.row(i) = Y.row(indices[i]);
	}
};
//
//template<>
//inline void extractBatch<typename Lynx::Matrix>(
//	Lynx::Matrix& XX,
//	Lynx::Matrix& YY,
//	const Lynx::Matrix& X,
//	const Lynx::Matrix& Y,
//	const std::vector<u64>& indices)
//{
//	if (XX.rows() != indices.size()) throw std::runtime_error(LOCATION);
//	//if (YY.rows() != indices.size()) throw std::runtime_error(LOCATION);
//	if (Y.cols() != 1)throw std::runtime_error(LOCATION);
//	//if (YY.cols() != 1)throw std::runtime_error(LOCATION);
//
//	auto destOffset = 0;
//	auto step = XX.cols();
//
//
//
//	for (u64 i = 0; i < indices.size(); ++i)
//	{
//		auto srcOffset = step * indices[i];
//		memcpy(XX.mShares[0].data() + destOffset, X.mShares[0].data() + srcOffset, step);
//		memcpy(XX.mShares[1].data() + destOffset, X.mShares[1].data() + srcOffset, step);
//		destOffset += step;
//
//		YY.mShares[0](i) = Y.mShares[0](indices[i]);
//		YY.mShares[1](i) = Y.mShares[1](indices[i]);
//
//		//XX.row(i) = X.row(indices[i]);
//		//YY.row(i) = Y.row(indices[i]);
//	}
//}
//

template<typename Engine, typename Matrix>
double test_linearModel(
	Engine& engine,
	Matrix& W,
	Matrix& x,
	Matrix& y)
{
	Matrix error = engine.mul(x, W) - y;
	Matrix errorT = error.transpose();
	Matrix l2 = engine.mul(errorT, error);


	return engine.reveal(l2(0)) / (double)y.rows();
}



template<typename Engine, typename Matrix>
void SGD_Linear(
	RegressionParam& params,
	Engine& engine,
	Matrix& X,
	Matrix& Y,
	Matrix& w,

	// optional
	Matrix* X_test = nullptr,
	Matrix* Y_test = nullptr)
{
	if (X.rows() != Y.rows() || Y.cols() != 1)
		throw std::runtime_error(LOCATION);

	// A random nummber generator used to select mini-batches
	PRNG prng(toBlock(234543234));

	// used to keep track of sampling mini-batches without replacement
	std::vector<u64> indices(X.rows()), batchIndices(params.mBatchSize);
	for (u64 i = 0; i < indices.size(); ++i) indices[i] = i;
	auto idxIter = indices.end();

	// the current mini-batch data
	Matrix XX(params.mBatchSize, X.cols());
	Matrix YY(params.mBatchSize, 1);

	// the learning rate in log2 form. We will truncate this many bits.
	u64 aB = std::log2(1/(params.mLearningRate / params.mBatchSize));
	auto start = std::chrono::system_clock::now();

	for (u64 i = 0; i < params.mIterations; ++i)
	{
		// sample the next mini-batch without replacement.
		getSubset(batchIndices, indices, idxIter, prng);

		XX.resize(params.mBatchSize, X.cols());
		// extract the rows indexed by batchIndices and store them in XX, YY.

		extractBatch(XX, YY, X, Y, batchIndices);

		DEBUG_PRINT(engine << "X[" << i << "] " << engine.reveal(XX) << std::endl);
		DEBUG_PRINT(engine << "Y[" << i << "] " << engine.reveal(YY) << std::endl);
		DEBUG_PRINT(engine << "W[" << i << "] " << engine.reveal(w) << std::endl);

		// compute the errors on the current batch.
		Matrix error = engine.mul(XX, w);
		error -= YY;

		DEBUG_PRINT(engine << "E[" << i << "] " << engine.reveal(error) << std::endl);

		// compute XX = XX^T
		XX.transposeInPlace();

		// apply the update function  w = w - a/|B| (XX^T * (XX * w - YY))
		Matrix update = engine.mulTruncate(XX, error, aB);
		//std::cout << update << std::endl;
		w = w - update;

		DEBUG_PRINT(engine << "U[" << i << "] " << engine.reveal(update) << std::endl);
		DEBUG_PRINT(engine << "W[" << i << "] " << engine.reveal(w) << std::endl);


		if (X_test && i % 1000 == 0)
		{
			//engine.sync(); 
			auto now = std::chrono::system_clock::now();
			auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
			auto score = test_linearModel(engine, w, *X_test, *Y_test);

			if(engine.partyIdx() == 0)
				lout << i << " @ " << ((i+1) * 1000.0 / dur) <<" iters/s " << Color::Green << score  << std::endl << Color::Default;// << << std::endl;
		}
	}
}


template<typename Engine, typename Matrix>
std::array<double,2> test_logisticModel(
	Engine& engine,
	Matrix& W,
	Matrix& x,
	Matrix& y)
{
	auto xw = engine.mul(x, W);
	auto fxw = engine.logisticFunc(xw);
	Matrix error = fxw - y;
	Matrix errorT = error.transpose();
	Matrix l2 = engine.mul(errorT, error);


	auto pp = engine.reveal(fxw);
	auto yy = engine.reveal(y);
	u64 count = 0;
	for (u64 i = 0; i < fxw.size(); ++i)
	{
		bool c0 = pp(i) > 0.5;
		bool c1 = yy(i) > 0.5;

		count += (c0 == c1);
	}

	return { engine.reveal(l2(0)) / (double)y.rows(), count / (double)y.rows() };
}



template<typename Engine, typename Matrix>
void SGD_Logistic(
	RegressionParam& params,
	Engine& engine,
	Matrix& X,
	Matrix& Y,
	Matrix& w,

	// optional
	Matrix* X_test = nullptr,
	Matrix* Y_test = nullptr)
{
	if (X.rows() != Y.rows() || Y.cols() != 1)
		throw std::runtime_error(LOCATION);

	// A random nummber generator used to select mini-batches
	PRNG prng(toBlock(234543234));

	// used to keep track of sampling mini-batches without replacement
	std::vector<u64> indices(X.rows()), batchIndices(params.mBatchSize);
	for (u64 i = 0; i < indices.size(); ++i) indices[i] = i;
	auto idxIter = indices.end();

	// the current mini-batch data
	Matrix XX(params.mBatchSize, X.cols());
	Matrix YY(params.mBatchSize, 1);

	// the learning rate in log2 form. We will truncate this many bits.
	u64 aB = std::log2(1 / (params.mLearningRate / params.mBatchSize));
	auto start = std::chrono::system_clock::now();

	for (u64 i = 0; i < params.mIterations; ++i)
	{
		// sample the next mini-batch without replacement.
		getSubset(batchIndices, indices, idxIter, prng);

		XX.resize(params.mBatchSize, X.cols());
		// extract the rows indexed by batchIndices and store them in XX, YY.
		extractBatch(XX, YY, X, Y, batchIndices);

		DEBUG_PRINT(engine << "X[" << i << "] " << engine.reveal(XX) << std::endl);
		DEBUG_PRINT(engine << "Y[" << i << "] " << engine.reveal(YY) << std::endl);
		DEBUG_PRINT(engine << "W[" << i << "] " << engine.reveal(w) << std::endl);

		// compute the errors on the current batch.
		Matrix xw = engine.mul(XX, w);
		Matrix fxw = engine.logisticFunc(xw);

		DEBUG_PRINT(engine << "P[" << i << "] " << engine.reveal(xw) << std::endl);
		DEBUG_PRINT(engine << "F[" << i << "] " << engine.reveal(fxw) << std::endl);

		Matrix error = fxw - YY;

		DEBUG_PRINT(engine << "E[" << i << "] " << engine.reveal(error) << std::endl);

		// compute XX = XX^T
		XX.transposeInPlace();

		// apply the update function  w = w - a/|B| (XX^T * (XX * w - YY))
		Matrix update = engine.mulTruncate(XX, error, aB);

		//std::cout<< "U["<<i<<"] = " << update(0) << std::endl;
		w = w - update;

		DEBUG_PRINT(engine << "U[" << i << "] " << engine.reveal(update) << std::endl);

		if (X_test && i % 10 == 0)
		{

			auto now = std::chrono::system_clock::now();
			auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
			auto score = test_logisticModel(engine, w, *X_test, *Y_test);
			auto l2 = score[0];
			auto percent = score[1];
			lout << i << " @ " << ((i + 1) * 1000.0 / dur) << " iters/s  " << Color::Green<< l2 << " " << percent<< std::endl << Color::Default;
		}
	}
}


template<typename Eng>
typename Eng::Matrix pred_neural(Eng& eng, typename Eng::Matrix& X, std::vector<typename Eng::Matrix>& W)
{

	auto layers = W.size() - 1;

	auto Xi = X;
	for (u64 i = 0; i < layers; ++i)
	{
		auto Y = eng.mul(Xi, W[i]);
		Xi = eng.reluFunc(Y);
	}

	auto Y = eng.mul(Xi, W.back());
	Y = eng.argMax(Y);

	return std::move(Y);
}




template<typename Eng>
typename Eng::Matrix pred_linear(Eng& eng, typename Eng::Matrix& X, typename Eng::Matrix& W)
{
	auto Y = eng.mul(X, W);
	return std::move(Y);
}


template<typename Eng>
typename Eng::Matrix pred_logistic(Eng& eng, typename Eng::Matrix& X, typename Eng::Matrix& W)
{
	auto xw = eng.mul(X, W);
	auto fxw = eng.extractSign(xw);
	return std::move(fxw);
}


