/**
 * Copyright (c) 2011 10gen Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/accumulator.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/value.h"

namespace mongo {

    using boost::intrusive_ptr;

namespace {
    const char subTotalName[] = "subTotal";
    const char countName[] = "count";
}

    void AccumulatorAngleAvg::processInternal(const Value& input, bool merging) {
        if (!merging) {
            // non numeric types have no impact on average
            if (!input.numeric())
                return;
            if(input.getDouble() < 180){
                _smallAngles += input.getDouble();
                _smallCount += 1;
            }else{
                _largeAngles += input.getDouble();
                _largeCount += 1;
            }
        }
        else {
            // We expect an object that contains both a subtotal and a count.
            // This is what getValue(true) produced below.
            verify(input.getType() == Object);
            if(input[subTotalName].getDouble() < 180){
                _smallAngles += input[subTotalName].getDouble();
                _smallCount += input[countName].getLong();
            }else{
                _largeAngles += input[subTotalName].getDouble();
                _largeCount += input[countName].getLong();
            }
        }
    }

    intrusive_ptr<Accumulator> AccumulatorAngleAvg::create() {
        return new AccumulatorAngleAvg();
    }

    Value AccumulatorAngleAvg::getValue(bool toBeMerged) const {
        if (!toBeMerged) {
            double smallAvg = 0;
            double largeAvg = 0;
            double average = 0;
            if(_smallCount > 0){
                smallAvg = _smallAngles /static_cast<double>(_smallCount);
            }
            if(_largeCount > 0){
                largeAvg = _largeAngles /static_cast<double>(_largeCount);
            }
            
            if (_smallCount == 0 ){
                return Value(largeAvg);
            }
            
            if (_largeCount == 0){
                return Value(smallAvg);
            }
            
            
            average = (smallAvg * _smallCount + largeAvg * _largeCount) / static_cast<double>(_smallCount + _largeCount);
            if (largeAvg < smallAvg + 180){
                return Value(average);
            }else if(largeAvg > smallAvg + 180){
                return Value(fmod(average + 180,360));
            }else{
                if (_smallCount > _largeCount){
                    return Value(smallAvg);
                }else if(_smallCount < _largeCount){
                    return Value(largeAvg);
                }else{
                    return Value(0);
                }
            }

       
        }
        else {
            return Value(0);
        }
    }

    AccumulatorAngleAvg::AccumulatorAngleAvg()
        : _smallAngles(0)
        , _largeAngles(0)
        , _smallCount(0)
        , _largeCount(0)
    
    {
        // This is a fixed size Accumulator so we never need to update this
        _memUsageBytes = sizeof(*this);
    }

    void AccumulatorAngleAvg::reset() {
        _smallAngles = 0;
        _largeAngles = 0;
        _smallCount = 0;
        _largeCount = 0;
       
    }

    const char *AccumulatorAngleAvg::getOpName() const {
        return "$angleAvg";
    }
}
